/*
* Copyright 2014 The Netty Project
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
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class HttpResponseEncoderTest {
    private static final long INTEGER_OVERFLOW = (long) Integer.MAX_VALUE + 1;
    private static final FileRegion FILE_REGION = new DummyLongFileRegion();

    @Test
    public void testLargeFileRegionChunked() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        assertTrue(channel.writeOutbound(response));

        ByteBuf buffer = channel.readOutbound();

        assertEquals("HTTP/1.1 200 OK\r\n" + HttpHeaderNames.TRANSFER_ENCODING + ": " +
                HttpHeaderValues.CHUNKED + "\r\n\r\n", buffer.toString(CharsetUtil.US_ASCII));
        buffer.release();
        assertTrue(channel.writeOutbound(FILE_REGION));
        buffer = channel.readOutbound();
        assertEquals("80000000\r\n", buffer.toString(CharsetUtil.US_ASCII));
        buffer.release();

        FileRegion region = channel.readOutbound();
        assertSame(FILE_REGION, region);
        region.release();
        buffer = channel.readOutbound();
        assertEquals("\r\n", buffer.toString(CharsetUtil.US_ASCII));
        buffer.release();

        assertTrue(channel.writeOutbound(LastHttpContent.EMPTY_LAST_CONTENT));
        buffer = channel.readOutbound();
        assertEquals("0\r\n\r\n", buffer.toString(CharsetUtil.US_ASCII));
        buffer.release();

        assertFalse(channel.finish());
    }

    private static class DummyLongFileRegion implements FileRegion {

        @Override
        public long position() {
            return 0;
        }

        @Override
        public long transfered() {
            return 0;
        }

        @Override
        public long transferred() {
            return 0;
        }

        @Override
        public long count() {
            return INTEGER_OVERFLOW;
        }

        @Override
        public long transferTo(WritableByteChannel target, long position) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileRegion touch(Object hint) {
            return this;
        }

        @Override
        public FileRegion touch() {
            return this;
        }

        @Override
        public FileRegion retain() {
            return this;
        }

        @Override
        public FileRegion retain(int increment) {
            return this;
        }

        @Override
        public int refCnt() {
            return 1;
        }

        @Override
        public boolean release() {
            return false;
        }

        @Override
        public boolean release(int decrement) {
            return false;
        }
    }

    @Test
    public void testEmptyBufferBypass() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder());

        // Test writing an empty buffer works when the encoder is at ST_INIT.
        channel.writeOutbound(Unpooled.EMPTY_BUFFER);
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer, is(sameInstance(Unpooled.EMPTY_BUFFER)));

        // Leave the ST_INIT state.
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        assertTrue(channel.writeOutbound(response));
        buffer = channel.readOutbound();
        assertEquals("HTTP/1.1 200 OK\r\n\r\n", buffer.toString(CharsetUtil.US_ASCII));
        buffer.release();

        // Test writing an empty buffer works when the encoder is not at ST_INIT.
        channel.writeOutbound(Unpooled.EMPTY_BUFFER);
        buffer = channel.readOutbound();
        assertThat(buffer, is(sameInstance(Unpooled.EMPTY_BUFFER)));

        assertFalse(channel.finish());
    }

    @Test
    public void testEmptyContentChunked() throws Exception {
        testEmptyContent(true);
    }

    @Test
    public void testEmptyContentNotChunked() throws Exception {
        testEmptyContent(false);
    }

    private static void testEmptyContent(boolean chunked) throws Exception {
        String content = "netty rocks";
        ByteBuf contentBuffer = Unpooled.copiedBuffer(content, CharsetUtil.US_ASCII);
        int length = contentBuffer.readableBytes();

        EmbeddedChannel channel = new EmbeddedChannel(new HttpResponseEncoder());
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        if (!chunked) {
            HttpUtil.setContentLength(response, length);
        }
        assertTrue(channel.writeOutbound(response));
        assertTrue(channel.writeOutbound(new DefaultHttpContent(Unpooled.EMPTY_BUFFER)));
        assertTrue(channel.writeOutbound(new DefaultLastHttpContent(contentBuffer)));

        ByteBuf buffer = channel.readOutbound();
        if (!chunked) {
            assertEquals("HTTP/1.1 200 OK\r\ncontent-length: " + length + "\r\n\r\n",
                    buffer.toString(CharsetUtil.US_ASCII));
        } else {
            assertEquals("HTTP/1.1 200 OK\r\n\r\n", buffer.toString(CharsetUtil.US_ASCII));
        }
        buffer.release();

        // Test writing an empty buffer works when the encoder is not at ST_INIT.
        buffer = channel.readOutbound();
        assertEquals(0, buffer.readableBytes());
        buffer.release();

        buffer = channel.readOutbound();
        assertEquals(length, buffer.readableBytes());
        buffer.release();

        assertFalse(channel.finish());
    }
}
