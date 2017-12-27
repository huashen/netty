/*
 * Copyright 2016 The Netty Project
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

import io.netty.util.AsciiString;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static io.netty.handler.codec.http2.DefaultHttp2HeadersTest.verifyPseudoHeadersFirst;
import static org.junit.Assert.*;

public class ReadOnlyHttp2HeadersTest {
    @Test(expected = IllegalArgumentException.class)
    public void notKeyValuePairThrows() {
        ReadOnlyHttp2Headers.trailers(false, new AsciiString[]{ null });
    }

    @Test(expected = NullPointerException.class)
    public void nullTrailersNotAllowed() {
        ReadOnlyHttp2Headers.trailers(false, (AsciiString[]) null);
    }

    @Test
    public void nullHeaderNameNotChecked() {
        ReadOnlyHttp2Headers.trailers(false, null, null);
    }

    @Test(expected = Http2Exception.class)
    public void nullHeaderNameValidated() {
        ReadOnlyHttp2Headers.trailers(true, null, new AsciiString("foo"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void pseudoHeaderNotAllowedAfterNonPseudoHeaders() {
        ReadOnlyHttp2Headers.trailers(true, new AsciiString(":name"), new AsciiString("foo"),
                                      new AsciiString("othername"), new AsciiString("goo"),
                                      new AsciiString(":pseudo"), new AsciiString("val"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullValuesAreNotAllowed() {
        ReadOnlyHttp2Headers.trailers(true, new AsciiString("foo"), null);
    }

    @Test
    public void emptyHeaderNameAllowed() {
        ReadOnlyHttp2Headers.trailers(false, AsciiString.EMPTY_STRING, new AsciiString("foo"));
    }

    @Test
    public void testPseudoHeadersMustComeFirstWhenIteratingServer() {
        Http2Headers headers = newServerHeaders();
        verifyPseudoHeadersFirst(headers);
    }

    @Test
    public void testPseudoHeadersMustComeFirstWhenIteratingClient() {
        Http2Headers headers = newClientHeaders();
        verifyPseudoHeadersFirst(headers);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorReadOnlyClient() {
        testIteratorReadOnly(newClientHeaders());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorReadOnlyServer() {
        testIteratorReadOnly(newServerHeaders());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorReadOnlyTrailers() {
        testIteratorReadOnly(newTrailers());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorEntryReadOnlyClient() {
        testIteratorEntryReadOnly(newClientHeaders());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorEntryReadOnlyServer() {
        testIteratorEntryReadOnly(newServerHeaders());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorEntryReadOnlyTrailers() {
        testIteratorEntryReadOnly(newTrailers());
    }

    @Test
    public void testSize() {
        Http2Headers headers = newTrailers();
        assertEquals(otherHeaders().length / 2, headers.size());
    }

    @Test
    public void testIsNotEmpty() {
        Http2Headers headers = newTrailers();
        assertFalse(headers.isEmpty());
    }

    @Test
    public void testIsEmpty() {
        Http2Headers headers = ReadOnlyHttp2Headers.trailers(false);
        assertTrue(headers.isEmpty());
    }

    @Test
    public void testContainsName() {
        Http2Headers headers = newClientHeaders();
        assertTrue(headers.contains("Name1"));
        assertTrue(headers.contains(Http2Headers.PseudoHeaderName.PATH.value()));
        assertFalse(headers.contains(Http2Headers.PseudoHeaderName.STATUS.value()));
        assertFalse(headers.contains("a missing header"));
    }

    @Test
    public void testContainsNameAndValue() {
        Http2Headers headers = newClientHeaders();
        assertTrue(headers.contains("Name1", "Value1"));
        assertTrue(headers.contains(Http2Headers.PseudoHeaderName.PATH.value(), "/foo"));
        assertFalse(headers.contains(Http2Headers.PseudoHeaderName.STATUS.value(), "200"));
        assertFalse(headers.contains("a missing header", "a missing value"));
    }

    @Test
    public void testGet() {
        Http2Headers headers = newClientHeaders();
        assertTrue(AsciiString.contentEqualsIgnoreCase("value1", headers.get("Name1")));
        assertTrue(AsciiString.contentEqualsIgnoreCase("/foo",
                   headers.get(Http2Headers.PseudoHeaderName.PATH.value())));
        assertNull(headers.get(Http2Headers.PseudoHeaderName.STATUS.value()));
        assertNull(headers.get("a missing header"));
    }

    private void testIteratorReadOnly(Http2Headers headers) {
        Iterator<Map.Entry<CharSequence, CharSequence>> itr = headers.iterator();
        assertTrue(itr.hasNext());
        itr.remove();
    }

    private void testIteratorEntryReadOnly(Http2Headers headers) {
        Iterator<Map.Entry<CharSequence, CharSequence>> itr = headers.iterator();
        assertTrue(itr.hasNext());
        itr.next().setValue("foo");
    }

    private ReadOnlyHttp2Headers newServerHeaders() {
        return ReadOnlyHttp2Headers.serverHeaders(false, new AsciiString("200"), otherHeaders());
    }

    private ReadOnlyHttp2Headers newClientHeaders() {
        return ReadOnlyHttp2Headers.clientHeaders(false, new AsciiString("meth"), new AsciiString("/foo"),
                new AsciiString("schemer"), new AsciiString("respect_my_authority"), otherHeaders());
    }

    private ReadOnlyHttp2Headers newTrailers() {
        return ReadOnlyHttp2Headers.trailers(false, otherHeaders());
    }

    private AsciiString[] otherHeaders() {
        return new AsciiString[] {
                new AsciiString("name1"), new AsciiString("value1"),
                new AsciiString("name2"), new AsciiString("value2"),
                new AsciiString("name3"), new AsciiString("value3")
        };
    }
}
