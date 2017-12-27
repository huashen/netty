/*
 * Copyright 2015 The Netty Project
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

/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.StringUtil;
import org.junit.Before;
import org.junit.Test;

import static io.netty.handler.codec.http2.HpackDecoder.decodeULE128;
import static io.netty.handler.codec.http2.Http2HeadersEncoder.NEVER_SENSITIVE;
import static io.netty.util.AsciiString.EMPTY_STRING;
import static io.netty.util.AsciiString.of;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class HpackDecoderTest {
    private HpackDecoder hpackDecoder;
    private Http2Headers mockHeaders;

    private static String hex(String s) {
        return StringUtil.toHexString(s.getBytes());
    }

    private void decode(String encoded) throws Http2Exception {
        byte[] b = StringUtil.decodeHexDump(encoded);
        ByteBuf in = Unpooled.wrappedBuffer(b);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Before
    public void setUp() throws Http2Exception {
        hpackDecoder = new HpackDecoder(8192, 32);
        mockHeaders = mock(Http2Headers.class);
    }

    @Test
    public void testDecodeULE128IntMax() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x07};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            assertEquals(MAX_VALUE, decodeULE128(in, 0));
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testDecodeULE128IntOverflow1() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x07};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        final int readerIndex = in.readerIndex();
        try {
            decodeULE128(in, 1);
        } finally {
            assertEquals(readerIndex, in.readerIndex());
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testDecodeULE128IntOverflow2() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x08};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        final int readerIndex = in.readerIndex();
        try {
            decodeULE128(in, 0);
        } finally {
            assertEquals(readerIndex, in.readerIndex());
            in.release();
        }
    }

    @Test
    public void testDecodeULE128LongMax() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0x7F};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            assertEquals(Long.MAX_VALUE, decodeULE128(in, 0L));
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testDecodeULE128LongOverflow1() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        final int readerIndex = in.readerIndex();
        try {
            decodeULE128(in, 0L);
        } finally {
            assertEquals(readerIndex, in.readerIndex());
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testDecodeULE128LongOverflow2() throws Http2Exception {
        byte[] input = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0x7F};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        final int readerIndex = in.readerIndex();
        try {
            decodeULE128(in, 1L);
        } finally {
            assertEquals(readerIndex, in.readerIndex());
            in.release();
        }
    }

    @Test
    public void testSetTableSizeWithMaxUnsigned32BitValueSucceeds() throws Http2Exception {
        byte[] input = {(byte) 0x3F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x0E};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            final long expectedHeaderSize = 4026531870L; // based on the input above
            hpackDecoder.setMaxHeaderTableSize(expectedHeaderSize);
            hpackDecoder.decode(0, in, mockHeaders);
            assertEquals(expectedHeaderSize, hpackDecoder.getMaxHeaderTableSize());
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testSetTableSizeOverLimitFails() throws Http2Exception {
        byte[] input = {(byte) 0x3F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x0E};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            hpackDecoder.setMaxHeaderTableSize(4026531870L - 1); // based on the input above ... 1 less than is above.
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Test
    public void testLiteralHuffmanEncodedWithEmptyNameAndValue() throws Http2Exception {
        byte[] input = {0, (byte) 0x80, 0};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
            verify(mockHeaders, times(1)).add(EMPTY_STRING, EMPTY_STRING);
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralHuffmanEncodedWithPaddingGreaterThan7Throws() throws Http2Exception {
        byte[] input = {0, (byte) 0x81, -1};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralHuffmanEncodedWithDecodingEOSThrows() throws Http2Exception {
        byte[] input = {0, (byte) 0x84, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralHuffmanEncodedWithPaddingNotCorrespondingToMSBThrows() throws Http2Exception {
        byte[] input = {0, (byte) 0x81, 0};
        ByteBuf in = Unpooled.wrappedBuffer(input);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testIncompleteIndex() throws Http2Exception {
        byte[] compressed = StringUtil.decodeHexDump("FFF0");
        ByteBuf in = Unpooled.wrappedBuffer(compressed);
        try {
            hpackDecoder.decode(0, in, mockHeaders);
            assertEquals(1, in.readableBytes());
            hpackDecoder.decode(0, in, mockHeaders);
        } finally {
            in.release();
        }
    }

    @Test(expected = Http2Exception.class)
    public void testUnusedIndex() throws Http2Exception {
        // Index 0 is not used
        decode("80");
    }

    @Test(expected = Http2Exception.class)
    public void testIllegalIndex() throws Http2Exception {
        // Index larger than the header table
        decode("FF00");
    }

    @Test(expected = Http2Exception.class)
    public void testInsidiousIndex() throws Http2Exception {
        // Insidious index so the last shift causes sign overflow
        decode("FF8080808007");
    }

    @Test
    public void testDynamicTableSizeUpdate() throws Http2Exception {
        decode("20");
        assertEquals(0, hpackDecoder.getMaxHeaderTableSize());
        decode("3FE11F");
        assertEquals(4096, hpackDecoder.getMaxHeaderTableSize());
    }

    @Test
    public void testDynamicTableSizeUpdateRequired() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(32);
        decode("3F00");
        assertEquals(31, hpackDecoder.getMaxHeaderTableSize());
    }

    @Test(expected = Http2Exception.class)
    public void testIllegalDynamicTableSizeUpdate() throws Http2Exception {
        // max header table size = MAX_HEADER_TABLE_SIZE + 1
        decode("3FE21F");
    }

    @Test(expected = Http2Exception.class)
    public void testInsidiousMaxDynamicTableSize() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(MAX_VALUE);
        // max header table size sign overflow
        decode("3FE1FFFFFF07");
    }

    @Test
    public void testMaxValidDynamicTableSize() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(MAX_VALUE);
        String baseValue = "3FE1FFFFFF0";
        for (int i = 0; i < 7; ++i) {
            decode(baseValue + i);
        }
    }

    @Test
    public void testReduceMaxDynamicTableSize() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(0);
        assertEquals(0, hpackDecoder.getMaxHeaderTableSize());
        decode("2081");
    }

    @Test(expected = Http2Exception.class)
    public void testTooLargeDynamicTableSizeUpdate() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(0);
        assertEquals(0, hpackDecoder.getMaxHeaderTableSize());
        decode("21"); // encoder max header table size not small enough
    }

    @Test(expected = Http2Exception.class)
    public void testMissingDynamicTableSizeUpdate() throws Http2Exception {
        hpackDecoder.setMaxHeaderTableSize(0);
        assertEquals(0, hpackDecoder.getMaxHeaderTableSize());
        decode("81");
    }

    @Test
    public void testLiteralWithIncrementalIndexingWithEmptyName() throws Http2Exception {
        decode("400005" + hex("value"));
        verify(mockHeaders, times(1)).add(EMPTY_STRING, of("value"));
    }

    @Test
    public void testLiteralWithIncrementalIndexingCompleteEviction() throws Http2Exception {
        // Verify indexed host header
        decode("4004" + hex("name") + "05" + hex("value"));
        verify(mockHeaders).add(of("name"), of("value"));
        verifyNoMoreInteractions(mockHeaders);

        reset(mockHeaders);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4096; i++) {
            sb.append('a');
        }
        String value = sb.toString();
        sb = new StringBuilder();
        sb.append("417F811F");
        for (int i = 0; i < 4096; i++) {
            sb.append("61"); // 'a'
        }
        decode(sb.toString());
        verify(mockHeaders).add(of(":authority"), of(value));
        verifyNoMoreInteractions(mockHeaders);
        reset(mockHeaders);

        // Verify next header is inserted at index 62
        decode("4004" + hex("name") + "05" + hex("value") + "BE");
        verify(mockHeaders, times(2)).add(of("name"), of("value"));
        verifyNoMoreInteractions(mockHeaders);
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralWithIncrementalIndexingWithLargeValue() throws Http2Exception {
        // Ignore header that exceeds max header size
        StringBuilder sb = new StringBuilder();
        sb.append("4004");
        sb.append(hex("name"));
        sb.append("7F813F");
        for (int i = 0; i < 8192; i++) {
            sb.append("61"); // 'a'
        }
        decode(sb.toString());
    }

    @Test
    public void testLiteralWithoutIndexingWithEmptyName() throws Http2Exception {
        decode("000005" + hex("value"));
        verify(mockHeaders, times(1)).add(EMPTY_STRING, of("value"));
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralWithoutIndexingWithLargeName() throws Http2Exception {
        // Ignore header name that exceeds max header size
        StringBuilder sb = new StringBuilder();
        sb.append("007F817F");
        for (int i = 0; i < 16384; i++) {
            sb.append("61"); // 'a'
        }
        sb.append("00");
        decode(sb.toString());
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralWithoutIndexingWithLargeValue() throws Http2Exception {
        // Ignore header that exceeds max header size
        StringBuilder sb = new StringBuilder();
        sb.append("0004");
        sb.append(hex("name"));
        sb.append("7F813F");
        for (int i = 0; i < 8192; i++) {
            sb.append("61"); // 'a'
        }
        decode(sb.toString());
    }

    @Test
    public void testLiteralNeverIndexedWithEmptyName() throws Http2Exception {
        decode("100005" + hex("value"));
        verify(mockHeaders, times(1)).add(EMPTY_STRING, of("value"));
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralNeverIndexedWithLargeName() throws Http2Exception {
        // Ignore header name that exceeds max header size
        StringBuilder sb = new StringBuilder();
        sb.append("107F817F");
        for (int i = 0; i < 16384; i++) {
            sb.append("61"); // 'a'
        }
        sb.append("00");
        decode(sb.toString());
    }

    @Test(expected = Http2Exception.class)
    public void testLiteralNeverIndexedWithLargeValue() throws Http2Exception {
        // Ignore header that exceeds max header size
        StringBuilder sb = new StringBuilder();
        sb.append("1004");
        sb.append(hex("name"));
        sb.append("7F813F");
        for (int i = 0; i < 8192; i++) {
            sb.append("61"); // 'a'
        }
        decode(sb.toString());
    }

    @Test
    public void testDecodeLargerThanMaxHeaderListSizeButSmallerThanMaxHeaderListSizeUpdatesDynamicTable()
        throws Http2Exception {
        ByteBuf in = Unpooled.buffer(200);
        try {
            hpackDecoder.setMaxHeaderListSize(100, 200);
            HpackEncoder hpackEncoder = new HpackEncoder(true);

            // encode headers that are slightly larger than maxHeaderListSize
            // but smaller than maxHeaderListSizeGoAway
            Http2Headers toEncode = new DefaultHttp2Headers();
            toEncode.add("test_1", "1");
            toEncode.add("test_2", "2");
            toEncode.add("long", String.format("%0100d", 0).replace('0', 'A'));
            toEncode.add("test_3", "3");
            hpackEncoder.encodeHeaders(1, in, toEncode, NEVER_SENSITIVE);

            // decode the headers, we should get an exception, but
            // the decoded headers object should contain all of the headers
            Http2Headers decoded = new DefaultHttp2Headers();
            try {
                hpackDecoder.decode(1, in, decoded);
                fail();
            } catch (Http2Exception e) {
                assertTrue(e instanceof Http2Exception.HeaderListSizeException);
            }

            assertEquals(4, decoded.size());
            assertTrue(decoded.contains("test_3"));
        } finally {
            in.release();
        }
    }
}
