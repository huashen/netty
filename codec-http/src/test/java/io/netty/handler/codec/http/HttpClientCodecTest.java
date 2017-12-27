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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.util.CharsetUtil;
import io.netty.util.NetUtil;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import static io.netty.util.ReferenceCountUtil.release;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpClientCodecTest {

    private static final String EMPTY_RESPONSE = "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n";
    private static final String RESPONSE = "HTTP/1.0 200 OK\r\n" + "Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n" +
            "Content-Type: text/html\r\n" + "Content-Length: 28\r\n" + "\r\n"
            + "<html><body></body></html>\r\n";
    private static final String INCOMPLETE_CHUNKED_RESPONSE = "HTTP/1.1 200 OK\r\n" + "Content-Type: text/plain\r\n" +
            "Transfer-Encoding: chunked\r\n" + "\r\n" +
            "5\r\n" + "first\r\n" + "6\r\n" + "second\r\n" + "0\r\n";
    private static final String CHUNKED_RESPONSE = INCOMPLETE_CHUNKED_RESPONSE + "\r\n";

    @Test
    public void testConnectWithResponseContent() {
        HttpClientCodec codec = new HttpClientCodec(4096, 8192, 8192, true);
        EmbeddedChannel ch = new EmbeddedChannel(codec);

        sendRequestAndReadResponse(ch, HttpMethod.CONNECT, RESPONSE);
        ch.finish();
    }

    @Test
    public void testFailsNotOnRequestResponseChunked() {
        HttpClientCodec codec = new HttpClientCodec(4096, 8192, 8192, true);
        EmbeddedChannel ch = new EmbeddedChannel(codec);

        sendRequestAndReadResponse(ch, HttpMethod.GET, CHUNKED_RESPONSE);
        ch.finish();
    }

    @Test
    public void testFailsOnMissingResponse() {
        HttpClientCodec codec = new HttpClientCodec(4096, 8192, 8192, true);
        EmbeddedChannel ch = new EmbeddedChannel(codec);

        assertTrue(ch.writeOutbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "http://localhost/")));
        ByteBuf buffer = ch.readOutbound();
        assertNotNull(buffer);
        buffer.release();
        try {
            ch.finish();
            fail();
        } catch (CodecException e) {
            assertTrue(e instanceof PrematureChannelClosureException);
        }
    }

    @Test
    public void testFailsOnIncompleteChunkedResponse() {
        HttpClientCodec codec = new HttpClientCodec(4096, 8192, 8192, true);
        EmbeddedChannel ch = new EmbeddedChannel(codec);

        ch.writeOutbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://localhost/"));
        ByteBuf buffer = ch.readOutbound();
        assertNotNull(buffer);
        buffer.release();
        assertNull(ch.readInbound());
        ch.writeInbound(Unpooled.copiedBuffer(INCOMPLETE_CHUNKED_RESPONSE, CharsetUtil.ISO_8859_1));
        assertThat(ch.readInbound(), instanceOf(HttpResponse.class));
        ((HttpContent) ch.readInbound()).release(); // Chunk 'first'
        ((HttpContent) ch.readInbound()).release(); // Chunk 'second'
        assertNull(ch.readInbound());

        try {
            ch.finish();
            fail();
        } catch (CodecException e) {
            assertTrue(e instanceof PrematureChannelClosureException);
        }
    }

    @Test
    public void testServerCloseSocketInputProvidesData() throws InterruptedException {
        ServerBootstrap sb = new ServerBootstrap();
        Bootstrap cb = new Bootstrap();
        final CountDownLatch serverChannelLatch = new CountDownLatch(1);
        final CountDownLatch responseReceivedLatch = new CountDownLatch(1);
        try {
            sb.group(new NioEventLoopGroup(2));
            sb.channel(NioServerSocketChannel.class);
            sb.childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    // Don't use the HttpServerCodec, because we don't want to have content-length or anything added.
                    ch.pipeline().addLast(new HttpRequestDecoder(4096, 8192, 8192, true));
                    ch.pipeline().addLast(new HttpObjectAggregator(4096));
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
                            // This is just a simple demo...don't block in IO
                            assertTrue(ctx.channel() instanceof SocketChannel);
                            final SocketChannel sChannel = (SocketChannel) ctx.channel();
                            /**
                             * The point of this test is to not add any content-length or content-encoding headers
                             * and the client should still handle this.
                             * See <a href="https://tools.ietf.org/html/rfc7230#section-3.3.3">RFC 7230, 3.3.3</a>.
                             */
                            sChannel.writeAndFlush(Unpooled.wrappedBuffer(("HTTP/1.0 200 OK\r\n" +
                            "Date: Fri, 31 Dec 1999 23:59:59 GMT\r\n" +
                            "Content-Type: text/html\r\n\r\n").getBytes(CharsetUtil.ISO_8859_1)))
                                    .addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    assertTrue(future.isSuccess());
                                    sChannel.writeAndFlush(Unpooled.wrappedBuffer(
                                            "<html><body>hello half closed!</body></html>\r\n"
                                            .getBytes(CharsetUtil.ISO_8859_1)))
                                            .addListener(new ChannelFutureListener() {
                                        @Override
                                        public void operationComplete(ChannelFuture future) throws Exception {
                                            assertTrue(future.isSuccess());
                                            sChannel.shutdownOutput();
                                        }
                                    });
                                }
                            });
                        }
                    });
                    serverChannelLatch.countDown();
                }
            });

            cb.group(new NioEventLoopGroup(1));
            cb.channel(NioSocketChannel.class);
            cb.option(ChannelOption.ALLOW_HALF_CLOSURE, true);
            cb.handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(new HttpClientCodec(4096, 8192, 8192, true, true));
                    ch.pipeline().addLast(new HttpObjectAggregator(4096));
                    ch.pipeline().addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
                            responseReceivedLatch.countDown();
                        }
                    });
                }
            });

            Channel serverChannel = sb.bind(new InetSocketAddress(0)).sync().channel();
            int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

            ChannelFuture ccf = cb.connect(new InetSocketAddress(NetUtil.LOCALHOST, port));
            assertTrue(ccf.awaitUninterruptibly().isSuccess());
            Channel clientChannel = ccf.channel();
            assertTrue(serverChannelLatch.await(5, SECONDS));
            clientChannel.writeAndFlush(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
            assertTrue(responseReceivedLatch.await(5, SECONDS));
        } finally {
            sb.config().group().shutdownGracefully();
            sb.config().childGroup().shutdownGracefully();
            cb.config().group().shutdownGracefully();
        }
    }

    @Test
    public void testContinueParsingAfterConnect() throws Exception {
        testAfterConnect(true);
    }

    @Test
    public void testPassThroughAfterConnect() throws Exception {
        testAfterConnect(false);
    }

    private static void testAfterConnect(final boolean parseAfterConnect) throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpClientCodec(4096, 8192, 8192, true, true, parseAfterConnect));

        Consumer connectResponseConsumer = new Consumer();
        sendRequestAndReadResponse(ch, HttpMethod.CONNECT, EMPTY_RESPONSE, connectResponseConsumer);
        assertTrue("No connect response messages received.", connectResponseConsumer.getReceivedCount() > 0);
        Consumer responseConsumer = new Consumer() {
            @Override
            void accept(Object object) {
                if (parseAfterConnect) {
                    assertThat("Unexpected response message type.", object, instanceOf(HttpObject.class));
                } else {
                    assertThat("Unexpected response message type.", object, not(instanceOf(HttpObject.class)));
                }
            }
        };
        sendRequestAndReadResponse(ch, HttpMethod.GET, RESPONSE, responseConsumer);
        assertTrue("No response messages received.", responseConsumer.getReceivedCount() > 0);
        assertFalse("Channel finish failed.", ch.finish());
    }

    private static void sendRequestAndReadResponse(EmbeddedChannel ch, HttpMethod httpMethod, String response) {
        sendRequestAndReadResponse(ch, httpMethod, response, new Consumer());
    }

    private static void sendRequestAndReadResponse(EmbeddedChannel ch, HttpMethod httpMethod, String response,
                                                   Consumer responseConsumer) {
        assertTrue("Channel outbound write failed.",
                ch.writeOutbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, "http://localhost/")));
        assertTrue("Channel inbound write failed.",
                ch.writeInbound(Unpooled.copiedBuffer(response, CharsetUtil.ISO_8859_1)));

        for (;;) {
            Object msg = ch.readOutbound();
            if (msg == null) {
                break;
            }
            release(msg);
        }
        for (;;) {
            Object msg = ch.readInbound();
            if (msg == null) {
                break;
            }
            responseConsumer.onResponse(msg);
            release(msg);
        }
    }

    private static class Consumer {

        private int receivedCount;

        final void onResponse(Object object) {
            receivedCount++;
            accept(object);
        }

        void accept(Object object) {
            // Default noop.
        }

        int getReceivedCount() {
            return receivedCount;
        }
    }
}
