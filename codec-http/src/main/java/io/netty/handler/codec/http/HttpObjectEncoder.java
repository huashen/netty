/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.StringUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http.HttpConstants.CR;
import static io.netty.handler.codec.http.HttpConstants.LF;

/**
 * Encodes an {@link HttpMessage} or an {@link HttpContent} into
 * a {@link ByteBuf}.
 *
 * <h3>Extensibility</h3>
 *
 * Please note that this encoder is designed to be extended to implement
 * a protocol derived from HTTP, such as
 * <a href="http://en.wikipedia.org/wiki/Real_Time_Streaming_Protocol">RTSP</a> and
 * <a href="http://en.wikipedia.org/wiki/Internet_Content_Adaptation_Protocol">ICAP</a>.
 * To implement the encoder of such a derived protocol, extend this class and
 * implement all abstract methods properly.
 */
public abstract class HttpObjectEncoder<H extends HttpMessage> extends MessageToMessageEncoder<Object> {
    static final int CRLF_SHORT = (CR << 8) | LF;
    private static final int ZERO_CRLF_MEDIUM = ('0' << 16) | CRLF_SHORT;
    private static final byte[] ZERO_CRLF_CRLF = { '0', CR, LF, CR, LF };
    private static final ByteBuf CRLF_BUF = unreleasableBuffer(directBuffer(2).writeByte(CR).writeByte(LF));
    private static final ByteBuf ZERO_CRLF_CRLF_BUF = unreleasableBuffer(directBuffer(ZERO_CRLF_CRLF.length)
            .writeBytes(ZERO_CRLF_CRLF));

    private static final int ST_INIT = 0;
    private static final int ST_CONTENT_NON_CHUNK = 1;
    private static final int ST_CONTENT_CHUNK = 2;
    private static final int ST_CONTENT_ALWAYS_EMPTY = 3;

    @SuppressWarnings("RedundantFieldInitialization")
    private int state = ST_INIT;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        ByteBuf buf = null;
        if (msg instanceof HttpMessage) {
            if (state != ST_INIT) {
                throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
            }

            @SuppressWarnings({ "unchecked", "CastConflictsWithInstanceof" })
            H m = (H) msg;

            buf = ctx.alloc().buffer();
            // Encode the message.
            encodeInitialLine(buf, m);
            encodeHeaders(m.headers(), buf);
            ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
            state = isContentAlwaysEmpty(m) ? ST_CONTENT_ALWAYS_EMPTY :
                    HttpUtil.isTransferEncodingChunked(m) ? ST_CONTENT_CHUNK : ST_CONTENT_NON_CHUNK;
        }

        // Bypass the encoder in case of an empty buffer, so that the following idiom works:
        //
        //     ch.write(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        //
        // See https://github.com/netty/netty/issues/2983 for more information.

        if (msg instanceof ByteBuf && !((ByteBuf) msg).isReadable()) {
            out.add(EMPTY_BUFFER);
            return;
        }

        if (msg instanceof HttpContent || msg instanceof ByteBuf || msg instanceof FileRegion) {
            switch (state) {
                case ST_INIT:
                    throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
                case ST_CONTENT_NON_CHUNK:
                    final long contentLength = contentLength(msg);
                    if (contentLength > 0) {
                        if (buf != null && buf.writableBytes() >= contentLength && msg instanceof HttpContent) {
                            // merge into other buffer for performance reasons
                            buf.writeBytes(((HttpContent) msg).content());
                            out.add(buf);
                        } else {
                            if (buf != null) {
                                out.add(buf);
                            }
                            out.add(encodeAndRetain(msg));
                        }

                        if (msg instanceof LastHttpContent) {
                            state = ST_INIT;
                        }

                        break;
                    }
                    // fall-through!
                case ST_CONTENT_ALWAYS_EMPTY:

                    if (buf != null) {
                        // We allocated a buffer so add it now.
                        out.add(buf);
                    } else {
                        // Need to produce some output otherwise an
                        // IllegalStateException will be thrown
                        out.add(EMPTY_BUFFER);
                    }

                    break;
                case ST_CONTENT_CHUNK:
                    if (buf != null) {
                        // We allocated a buffer so add it now.
                        out.add(buf);
                    }
                    encodeChunkedContent(ctx, msg, contentLength(msg), out);

                    break;
                default:
                    throw new Error();
            }

            if (msg instanceof LastHttpContent) {
                state = ST_INIT;
            }
        } else if (buf != null) {
            out.add(buf);
        }
    }

    /**
     * Encode the {@link HttpHeaders} into a {@link ByteBuf}.
     */
    protected void encodeHeaders(HttpHeaders headers, ByteBuf buf) {
        Iterator<Entry<CharSequence, CharSequence>> iter = headers.iteratorCharSequence();
        while (iter.hasNext()) {
            Entry<CharSequence, CharSequence> header = iter.next();
            HttpHeadersEncoder.encoderHeader(header.getKey(), header.getValue(), buf);
        }
    }

    private void encodeChunkedContent(ChannelHandlerContext ctx, Object msg, long contentLength, List<Object> out) {
        if (contentLength > 0) {
            String lengthHex = Long.toHexString(contentLength);
            ByteBuf buf = ctx.alloc().buffer(lengthHex.length() + 2);
            buf.writeCharSequence(lengthHex, CharsetUtil.US_ASCII);
            ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
            out.add(buf);
            out.add(encodeAndRetain(msg));
            out.add(CRLF_BUF.duplicate());
        }

        if (msg instanceof LastHttpContent) {
            HttpHeaders headers = ((LastHttpContent) msg).trailingHeaders();
            if (headers.isEmpty()) {
                out.add(ZERO_CRLF_CRLF_BUF.duplicate());
            } else {
                ByteBuf buf = ctx.alloc().buffer();
                ByteBufUtil.writeMediumBE(buf, ZERO_CRLF_MEDIUM);
                encodeHeaders(headers, buf);
                ByteBufUtil.writeShortBE(buf, CRLF_SHORT);
                out.add(buf);
            }
        } else if (contentLength == 0) {
            // Need to produce some output otherwise an
            // IllegalStateException will be thrown
            out.add(EMPTY_BUFFER);
        }
    }

    /**
     * Determine whether a message has a content or not. Some message may have headers indicating
     * a content without having an actual content, e.g the response to an HEAD or CONNECT request.
     *
     * @param msg the message to test
     * @return {@code true} to signal the message has no content
     */
    protected boolean isContentAlwaysEmpty(@SuppressWarnings("unused") H msg) {
        return false;
    }

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return msg instanceof HttpObject || msg instanceof ByteBuf || msg instanceof FileRegion;
    }

    private static Object encodeAndRetain(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).retain();
        }
        if (msg instanceof HttpContent) {
            return ((HttpContent) msg).content().retain();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).retain();
        }
        throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
    }

    private static long contentLength(Object msg) {
        if (msg instanceof HttpContent) {
            return ((HttpContent) msg).content().readableBytes();
        }
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
    }

    @Deprecated
    protected static void encodeAscii(String s, ByteBuf buf) {
        buf.writeCharSequence(s, CharsetUtil.US_ASCII);
    }

    protected abstract void encodeInitialLine(ByteBuf buf, H message) throws Exception;
}
