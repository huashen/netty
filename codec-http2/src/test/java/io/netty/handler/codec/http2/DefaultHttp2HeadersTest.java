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

package io.netty.handler.codec.http2;

import io.netty.handler.codec.http2.Http2Headers.PseudoHeaderName;
import io.netty.util.internal.StringUtil;
import org.junit.Test;

import java.util.Map.Entry;

import static io.netty.util.AsciiString.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DefaultHttp2HeadersTest {

    @Test(expected = Http2Exception.class)
    public void nullHeaderNameNotAllowed() {
        new DefaultHttp2Headers().add(null, "foo");
    }

    @Test(expected = Http2Exception.class)
    public void emptyHeaderNameNotAllowed() {
        new DefaultHttp2Headers().add(StringUtil.EMPTY_STRING, "foo");
    }

    @Test
    public void testPseudoHeadersMustComeFirstWhenIterating() {
        Http2Headers headers = newHeaders();

        verifyPseudoHeadersFirst(headers);
        verifyAllPseudoHeadersPresent(headers);
    }

    @Test
    public void testPseudoHeadersWithRemovePreservesPseudoIterationOrder() {
        Http2Headers headers = newHeaders();

        Http2Headers nonPseudoHeaders = new DefaultHttp2Headers();
        for (Entry<CharSequence, CharSequence> entry : headers) {
            if (entry.getKey().length() == 0 || entry.getKey().charAt(0) != ':' &&
                !nonPseudoHeaders.contains(entry.getKey())) {
                nonPseudoHeaders.add(entry.getKey(), entry.getValue());
            }
        }

        assertFalse(nonPseudoHeaders.isEmpty());

        // Remove all the non-pseudo headers and verify
        for (Entry<CharSequence, CharSequence> nonPseudoHeaderEntry : nonPseudoHeaders) {
            assertTrue(headers.remove(nonPseudoHeaderEntry.getKey()));
            verifyPseudoHeadersFirst(headers);
            verifyAllPseudoHeadersPresent(headers);
        }

        // Add back all non-pseudo headers
        for (Entry<CharSequence, CharSequence> nonPseudoHeaderEntry : nonPseudoHeaders) {
            headers.add(nonPseudoHeaderEntry.getKey(), of("goo"));
            verifyPseudoHeadersFirst(headers);
            verifyAllPseudoHeadersPresent(headers);
        }
    }

    @Test
    public void testPseudoHeadersWithClearDoesNotLeak() {
        Http2Headers headers = newHeaders();

        assertFalse(headers.isEmpty());
        headers.clear();
        assertTrue(headers.isEmpty());

        // Combine 2 headers together, make sure pseudo headers stay up front.
        headers.add("name1", "value1").scheme("nothing");
        verifyPseudoHeadersFirst(headers);

        Http2Headers other = new DefaultHttp2Headers().add("name2", "value2").authority("foo");
        verifyPseudoHeadersFirst(other);

        headers.add(other);
        verifyPseudoHeadersFirst(headers);

        // Make sure the headers are what we expect them to be, and no leaking behind the scenes.
        assertEquals(4, headers.size());
        assertEquals("value1", headers.get("name1"));
        assertEquals("value2", headers.get("name2"));
        assertEquals("nothing", headers.scheme());
        assertEquals("foo", headers.authority());
    }

    @Test
    public void testSetHeadersOrdersPseudoHeadersCorrectly() {
        Http2Headers headers = newHeaders();
        Http2Headers other = new DefaultHttp2Headers().add("name2", "value2").authority("foo");

        headers.set(other);
        verifyPseudoHeadersFirst(headers);
        assertEquals(other.size(), headers.size());
        assertEquals("foo", headers.authority());
        assertEquals("value2", headers.get("name2"));
    }

    @Test
    public void testSetAllOrdersPseudoHeadersCorrectly() {
        Http2Headers headers = newHeaders();
        Http2Headers other = new DefaultHttp2Headers().add("name2", "value2").authority("foo");

        int headersSizeBefore = headers.size();
        headers.setAll(other);
        verifyPseudoHeadersFirst(headers);
        verifyAllPseudoHeadersPresent(headers);
        assertEquals(headersSizeBefore + 1, headers.size());
        assertEquals("foo", headers.authority());
        assertEquals("value2", headers.get("name2"));
    }

    @Test(expected = Http2Exception.class)
    public void testHeaderNameValidation() {
        Http2Headers headers = newHeaders();

        headers.add(of("Foo"), of("foo"));
    }

    @Test
    public void testClearResetsPseudoHeaderDivision() {
        DefaultHttp2Headers http2Headers = new DefaultHttp2Headers();
        http2Headers.method("POST");
        http2Headers.set("some", "value");
        http2Headers.clear();
        http2Headers.method("GET");
        assertEquals(1, http2Headers.names().size());
    }

    private static void verifyAllPseudoHeadersPresent(Http2Headers headers) {
        for (PseudoHeaderName pseudoName : PseudoHeaderName.values()) {
            assertNotNull(headers.get(pseudoName.value()));
        }
    }

    static void verifyPseudoHeadersFirst(Http2Headers headers) {
        CharSequence lastNonPseudoName = null;
        for (Entry<CharSequence, CharSequence> entry: headers) {
            if (entry.getKey().length() == 0 || entry.getKey().charAt(0) != ':') {
                lastNonPseudoName = entry.getKey();
            } else if (lastNonPseudoName != null) {
                fail("All pseudo headers must be fist in iteration. Pseudo header " + entry.getKey() +
                        " is after a non pseudo header " + lastNonPseudoName);
            }
        }
    }

    private static Http2Headers newHeaders() {
        Http2Headers headers = new DefaultHttp2Headers();
        headers.add(of("name1"), of("value1"), of("value2"));
        headers.method(of("POST"));
        headers.add(of("2name"), of("value3"));
        headers.path(of("/index.html"));
        headers.status(of("200"));
        headers.authority(of("netty.io"));
        headers.add(of("name3"), of("value4"));
        headers.scheme(of("https"));
        return headers;
    }
}
