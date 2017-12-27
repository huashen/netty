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

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.junit.Test;

import static io.netty.handler.codec.http.HttpHeadersTestUtils.of;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpContentCompressorTest {

    @Test
    public void testGetTargetContentEncoding() throws Exception {
        HttpContentCompressor compressor = new HttpContentCompressor();

        String[] tests = {
            // Accept-Encoding -> Content-Encoding
            "", null,
            "*", "gzip",
            "*;q=0.0", null,
            "gzip", "gzip",
            "compress, gzip;q=0.5", "gzip",
            "gzip; q=0.5, identity", "gzip",
            "gzip ; q=0.1", "gzip",
            "gzip; q=0, deflate", "deflate",
            " deflate ; q=0 , *;q=0.5", "gzip",
        };
        for (int i = 0; i < tests.length; i += 2) {
            String acceptEncoding = tests[i];
            String contentEncoding = tests[i + 1];
            ZlibWrapper targetWrapper = compressor.determineWrapper(acceptEncoding);
            String targetEncoding = null;
            if (targetWrapper != null) {
                switch (targetWrapper) {
                case GZIP:
                    targetEncoding = "gzip";
                    break;
                case ZLIB:
                    targetEncoding = "deflate";
                    break;
                default:
                    fail();
                }
            }
            assertEquals(contentEncoding, targetEncoding);
        }
    }

    @Test
    public void testSplitContent() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        ch.writeOutbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("Hell", CharsetUtil.US_ASCII)));
        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("o, w", CharsetUtil.US_ASCII)));
        ch.writeOutbound(new DefaultLastHttpContent(Unpooled.copiedBuffer("orld", CharsetUtil.US_ASCII)));

        assertEncodedResponse(ch);

        HttpContent chunk;
        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("1f8b0800000000000000f248cdc901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("cad7512807000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("ca2fca4901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("0300c2a99ae70c000000"));
        assertThat(chunk, is(instanceOf(HttpContent.class)));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(chunk.content().isReadable(), is(false));
        assertThat(chunk, is(instanceOf(LastHttpContent.class)));
        chunk.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void testChunkedContent() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        HttpResponse res = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ch.writeOutbound(res);

        assertEncodedResponse(ch);

        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("Hell", CharsetUtil.US_ASCII)));
        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("o, w", CharsetUtil.US_ASCII)));
        ch.writeOutbound(new DefaultLastHttpContent(Unpooled.copiedBuffer("orld", CharsetUtil.US_ASCII)));

        HttpContent chunk;
        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("1f8b0800000000000000f248cdc901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("cad7512807000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("ca2fca4901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("0300c2a99ae70c000000"));
        assertThat(chunk, is(instanceOf(HttpContent.class)));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(chunk.content().isReadable(), is(false));
        assertThat(chunk, is(instanceOf(LastHttpContent.class)));
        chunk.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void testChunkedContentWithTrailingHeader() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        HttpResponse res = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        ch.writeOutbound(res);

        assertEncodedResponse(ch);

        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("Hell", CharsetUtil.US_ASCII)));
        ch.writeOutbound(new DefaultHttpContent(Unpooled.copiedBuffer("o, w", CharsetUtil.US_ASCII)));
        LastHttpContent content = new DefaultLastHttpContent(Unpooled.copiedBuffer("orld", CharsetUtil.US_ASCII));
        content.trailingHeaders().set(of("X-Test"), of("Netty"));
        ch.writeOutbound(content);

        HttpContent chunk;
        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("1f8b0800000000000000f248cdc901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("cad7512807000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("ca2fca4901000000ffff"));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("0300c2a99ae70c000000"));
        assertThat(chunk, is(instanceOf(HttpContent.class)));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(chunk.content().isReadable(), is(false));
        assertThat(chunk, is(instanceOf(LastHttpContent.class)));
        assertEquals("Netty", ((LastHttpContent) chunk).trailingHeaders().get(of("X-Test")));
        chunk.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void testFullContentWithContentLength() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        FullHttpResponse fullRes = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer("Hello, World", CharsetUtil.US_ASCII));
        fullRes.headers().set(HttpHeaderNames.CONTENT_LENGTH, fullRes.content().readableBytes());
        ch.writeOutbound(fullRes);

        HttpResponse res = ch.readOutbound();
        assertThat(res, is(not(instanceOf(HttpContent.class))));

        assertThat(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), is(nullValue()));
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), is("gzip"));

        long contentLengthHeaderValue = HttpUtil.getContentLength(res);
        long observedLength = 0;

        HttpContent c = ch.readOutbound();
        observedLength += c.content().readableBytes();
        assertThat(ByteBufUtil.hexDump(c.content()), is("1f8b0800000000000000f248cdc9c9d75108cf2fca4901000000ffff"));
        c.release();

        c = ch.readOutbound();
        observedLength += c.content().readableBytes();
        assertThat(ByteBufUtil.hexDump(c.content()), is("0300c6865b260c000000"));
        c.release();

        LastHttpContent last = ch.readOutbound();
        assertThat(last.content().readableBytes(), is(0));
        last.release();

        assertThat(ch.readOutbound(), is(nullValue()));
        assertEquals(contentLengthHeaderValue, observedLength);
    }

    @Test
    public void testFullContent() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        FullHttpResponse res = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
            Unpooled.copiedBuffer("Hello, World", CharsetUtil.US_ASCII));
        ch.writeOutbound(res);

        assertEncodedResponse(ch);
        HttpContent c = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(c.content()), is("1f8b0800000000000000f248cdc9c9d75108cf2fca4901000000ffff"));
        c.release();

        c = ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(c.content()), is("0300c6865b260c000000"));
        c.release();

        LastHttpContent last = ch.readOutbound();
        assertThat(last.content().readableBytes(), is(0));
        last.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    /**
     * If the length of the content is unknown, {@link HttpContentEncoder} should not skip encoding the content
     * even if the actual length is turned out to be 0.
     */
    @Test
    public void testEmptySplitContent() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        ch.writeOutbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        assertEncodedResponse(ch);

        ch.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
        HttpContent chunk = (HttpContent) ch.readOutbound();
        assertThat(ByteBufUtil.hexDump(chunk.content()), is("1f8b080000000000000003000000000000000000"));
        assertThat(chunk, is(instanceOf(HttpContent.class)));
        chunk.release();

        chunk = ch.readOutbound();
        assertThat(chunk.content().isReadable(), is(false));
        assertThat(chunk, is(instanceOf(LastHttpContent.class)));
        chunk.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    /**
     * If the length of the content is 0 for sure, {@link HttpContentEncoder} should skip encoding.
     */
    @Test
    public void testEmptyFullContent() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
        ch.writeOutbound(res);

        Object o = ch.readOutbound();
        assertThat(o, is(instanceOf(FullHttpResponse.class)));

        res = (FullHttpResponse) o;
        assertThat(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), is(nullValue()));

        // Content encoding shouldn't be modified.
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), is(nullValue()));
        assertThat(res.content().readableBytes(), is(0));
        assertThat(res.content().toString(CharsetUtil.US_ASCII), is(""));
        res.release();

        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void testEmptyFullContentWithTrailer() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(newRequest());

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
        res.trailingHeaders().set(of("X-Test"), of("Netty"));
        ch.writeOutbound(res);

        Object o = ch.readOutbound();
        assertThat(o, is(instanceOf(FullHttpResponse.class)));

        res = (FullHttpResponse) o;
        assertThat(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), is(nullValue()));

        // Content encoding shouldn't be modified.
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), is(nullValue()));
        assertThat(res.content().readableBytes(), is(0));
        assertThat(res.content().toString(CharsetUtil.US_ASCII), is(""));
        assertEquals("Netty", res.trailingHeaders().get(of("X-Test")));
        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void test100Continue() throws Exception {
        FullHttpRequest request = newRequest();
        HttpUtil.set100ContinueExpected(request, true);

        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(request);

        FullHttpResponse continueResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);

        ch.writeOutbound(continueResponse);

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
        res.trailingHeaders().set(of("X-Test"), of("Netty"));
        ch.writeOutbound(res);

        Object o = ch.readOutbound();
        assertThat(o, is(instanceOf(FullHttpResponse.class)));

        res = (FullHttpResponse) o;
        assertSame(continueResponse, res);
        res.release();

        o = ch.readOutbound();
        assertThat(o, is(instanceOf(FullHttpResponse.class)));

        res = (FullHttpResponse) o;
        assertThat(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), is(nullValue()));

        // Content encoding shouldn't be modified.
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), is(nullValue()));
        assertThat(res.content().readableBytes(), is(0));
        assertThat(res.content().toString(CharsetUtil.US_ASCII), is(""));
        assertEquals("Netty", res.trailingHeaders().get(of("X-Test")));
        assertThat(ch.readOutbound(), is(nullValue()));
    }

    @Test
    public void testTooManyResponses() throws Exception {
        FullHttpRequest request = newRequest();
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        ch.writeInbound(request);

        ch.writeOutbound(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER));

        try {
            ch.writeOutbound(new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER));
            fail();
        } catch (EncoderException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
        assertTrue(ch.finish());
        for (;;) {
            Object message = ch.readOutbound();
            if (message == null) {
                break;
            }
            ReferenceCountUtil.release(message);
        }
        for (;;) {
            Object message = ch.readInbound();
            if (message == null) {
                break;
            }
            ReferenceCountUtil.release(message);
        }
    }

    @Test
    public void testIdentity() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        assertTrue(ch.writeInbound(newRequest()));

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer("Hello, World", CharsetUtil.US_ASCII));
        int len = res.content().readableBytes();
        res.headers().set(HttpHeaderNames.CONTENT_LENGTH, len);
        res.headers().set(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.IDENTITY);
        assertTrue(ch.writeOutbound(res));

        FullHttpResponse response = (FullHttpResponse) ch.readOutbound();
        assertEquals(String.valueOf(len), response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals(HttpHeaderValues.IDENTITY.toString(), response.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertEquals("Hello, World", response.content().toString(CharsetUtil.US_ASCII));
        response.release();

        assertTrue(ch.finishAndReleaseAll());
    }

    @Test
    public void testCustomEncoding() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpContentCompressor());
        assertTrue(ch.writeInbound(newRequest()));

        FullHttpResponse res = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer("Hello, World", CharsetUtil.US_ASCII));
        int len = res.content().readableBytes();
        res.headers().set(HttpHeaderNames.CONTENT_LENGTH, len);
        res.headers().set(HttpHeaderNames.CONTENT_ENCODING, "ascii");
        assertTrue(ch.writeOutbound(res));

        FullHttpResponse response = (FullHttpResponse) ch.readOutbound();
        assertEquals(String.valueOf(len), response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertEquals("ascii", response.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        assertEquals("Hello, World", response.content().toString(CharsetUtil.US_ASCII));
        response.release();

        assertTrue(ch.finishAndReleaseAll());
    }

    private static FullHttpRequest newRequest() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, "gzip");
        return req;
    }

    private static void assertEncodedResponse(EmbeddedChannel ch) {
        Object o = ch.readOutbound();
        assertThat(o, is(instanceOf(HttpResponse.class)));

        HttpResponse res = (HttpResponse) o;
        assertThat(res, is(not(instanceOf(HttpContent.class))));
        assertThat(res.headers().get(HttpHeaderNames.TRANSFER_ENCODING), is("chunked"));
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_LENGTH), is(nullValue()));
        assertThat(res.headers().get(HttpHeaderNames.CONTENT_ENCODING), is("gzip"));
    }
}
