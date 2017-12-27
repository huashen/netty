/*
 * Copyright 2017 The Netty Project
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
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;

import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_HEADER_LIST_SIZE;
import static io.netty.handler.codec.http2.Http2CodecUtil.MAX_HEADER_TABLE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HpackEncoderTest {
    private HpackDecoder hpackDecoder;
    private HpackEncoder hpackEncoder;
    private Http2Headers mockHeaders;

    @Before
    public void setUp() throws Http2Exception {
        hpackEncoder = new HpackEncoder();
        hpackDecoder = new HpackDecoder(DEFAULT_HEADER_LIST_SIZE, 32);
        mockHeaders = mock(Http2Headers.class);
    }

    @Test
    public void testSetMaxHeaderTableSizeToMaxValue() throws Http2Exception {
        ByteBuf buf = Unpooled.buffer();
        hpackEncoder.setMaxHeaderTableSize(buf, MAX_HEADER_TABLE_SIZE);
        hpackDecoder.setMaxHeaderTableSize(MAX_HEADER_TABLE_SIZE);
        hpackDecoder.decode(0, buf, mockHeaders);
        assertEquals(MAX_HEADER_TABLE_SIZE, hpackDecoder.getMaxHeaderTableSize());
        buf.release();
    }

    @Test(expected = Http2Exception.class)
    public void testSetMaxHeaderTableSizeOverflow() throws Http2Exception {
        ByteBuf buf = Unpooled.buffer();
        try {
            hpackEncoder.setMaxHeaderTableSize(buf, MAX_HEADER_TABLE_SIZE + 1);
        } finally {
            buf.release();
        }
    }
}
