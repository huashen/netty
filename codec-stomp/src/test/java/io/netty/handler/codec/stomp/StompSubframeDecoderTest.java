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
package io.netty.handler.codec.stomp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class StompSubframeDecoderTest {

    private EmbeddedChannel channel;

    @Before
    public void setup() throws Exception {
        channel = new EmbeddedChannel(new StompSubframeDecoder());
    }

    @After
    public void teardown() throws Exception {
        assertFalse(channel.finish());
    }

    @Test
    public void testSingleFrameDecoding() {
        ByteBuf incoming = Unpooled.buffer();
        incoming.writeBytes(StompTestConstants.CONNECT_FRAME.getBytes());
        channel.writeInbound(incoming);

        StompHeadersSubframe frame = channel.readInbound();
        assertNotNull(frame);
        assertEquals(StompCommand.CONNECT, frame.command());

        StompContentSubframe content = channel.readInbound();
        assertSame(LastStompContentSubframe.EMPTY_LAST_CONTENT, content);
        content.release();

        Object o = channel.readInbound();
        assertNull(o);
    }

    @Test
    public void testSingleFrameWithBodyAndContentLength() {
        ByteBuf incoming = Unpooled.buffer();
        incoming.writeBytes(StompTestConstants.SEND_FRAME_2.getBytes());
        channel.writeInbound(incoming);

        StompHeadersSubframe frame = channel.readInbound();
        assertNotNull(frame);
        assertEquals(StompCommand.SEND, frame.command());

        StompContentSubframe content = channel.readInbound();
        assertTrue(content instanceof LastStompContentSubframe);
        String s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals("hello, queue a!!!", s);
        content.release();

        assertNull(channel.readInbound());
    }

    @Test
    public void testSingleFrameWithBodyWithoutContentLength() {
        ByteBuf incoming = Unpooled.buffer();
        incoming.writeBytes(StompTestConstants.SEND_FRAME_1.getBytes());
        channel.writeInbound(incoming);

        StompHeadersSubframe frame = channel.readInbound();
        assertNotNull(frame);
        assertEquals(StompCommand.SEND, frame.command());

        StompContentSubframe content = channel.readInbound();
        assertTrue(content instanceof LastStompContentSubframe);
        String s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals("hello, queue a!", s);
        content.release();

        assertNull(channel.readInbound());
    }

    @Test
    public void testSingleFrameChunked() {
        EmbeddedChannel channel = new EmbeddedChannel(new StompSubframeDecoder(10000, 5));

        ByteBuf incoming = Unpooled.buffer();
        incoming.writeBytes(StompTestConstants.SEND_FRAME_2.getBytes());
        channel.writeInbound(incoming);

        StompHeadersSubframe frame = channel.readInbound();
        assertNotNull(frame);
        assertEquals(StompCommand.SEND, frame.command());

        StompContentSubframe content = channel.readInbound();
        String s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals("hello", s);
        content.release();

        content = channel.readInbound();
        s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals(", que", s);
        content.release();

        content = channel.readInbound();
        s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals("ue a!", s);
        content.release();

        content = channel.readInbound();
        s = content.content().toString(CharsetUtil.UTF_8);
        assertEquals("!!", s);
        content.release();

        assertNull(channel.readInbound());
    }

    @Test
    public void testMultipleFramesDecoding() {
        ByteBuf incoming = Unpooled.buffer();
        incoming.writeBytes(StompTestConstants.CONNECT_FRAME.getBytes());
        incoming.writeBytes(StompTestConstants.CONNECTED_FRAME.getBytes());
        channel.writeInbound(incoming);

        StompHeadersSubframe frame = channel.readInbound();
        assertNotNull(frame);
        assertEquals(StompCommand.CONNECT, frame.command());

        StompContentSubframe content = channel.readInbound();
        assertSame(LastStompContentSubframe.EMPTY_LAST_CONTENT, content);
        content.release();

        StompHeadersSubframe frame2 = channel.readInbound();
        assertNotNull(frame2);
        assertEquals(StompCommand.CONNECTED, frame2.command());

        StompContentSubframe content2 = channel.readInbound();
        assertSame(LastStompContentSubframe.EMPTY_LAST_CONTENT, content2);
        content2.release();

        assertNull(channel.readInbound());
    }
}
