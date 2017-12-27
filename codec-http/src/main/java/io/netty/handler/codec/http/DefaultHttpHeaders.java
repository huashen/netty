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

import io.netty.handler.codec.CharSequenceValueConverter;
import io.netty.handler.codec.DateFormatter;
import io.netty.handler.codec.DefaultHeaders;
import io.netty.handler.codec.DefaultHeaders.NameValidator;
import io.netty.handler.codec.DefaultHeadersImpl;
import io.netty.handler.codec.HeadersUtils;
import io.netty.handler.codec.ValueConverter;
import io.netty.util.AsciiString;
import io.netty.util.ByteProcessor;
import io.netty.util.internal.PlatformDependent;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.netty.util.AsciiString.CASE_INSENSITIVE_HASHER;
import static io.netty.util.AsciiString.CASE_SENSITIVE_HASHER;

/**
 * Default implementation of {@link HttpHeaders}.
 */
public class DefaultHttpHeaders extends HttpHeaders {
    private static final int HIGHEST_INVALID_VALUE_CHAR_MASK = ~15;
    private static final ByteProcessor HEADER_NAME_VALIDATOR = new ByteProcessor() {
        @Override
        public boolean process(byte value) throws Exception {
            validateHeaderNameElement(value);
            return true;
        }
    };
    static final NameValidator<CharSequence> HttpNameValidator = new NameValidator<CharSequence>() {
        @Override
        public void validateName(CharSequence name) {
            if (name == null || name.length() == 0) {
                throw new IllegalArgumentException("empty headers are not allowed [" + name + "]");
            }
            if (name instanceof AsciiString) {
                try {
                    ((AsciiString) name).forEachByte(HEADER_NAME_VALIDATOR);
                } catch (Exception e) {
                    PlatformDependent.throwException(e);
                }
            } else {
                // Go through each character in the name
                for (int index = 0; index < name.length(); ++index) {
                    validateHeaderNameElement(name.charAt(index));
                }
            }
        }
    };

    private final DefaultHeaders<CharSequence, CharSequence, ?> headers;

    public DefaultHttpHeaders() {
        this(true);
    }

    public DefaultHttpHeaders(boolean validate) {
        this(validate, nameValidator(validate));
    }

    protected DefaultHttpHeaders(boolean validate, NameValidator<CharSequence> nameValidator) {
        this(new DefaultHeadersImpl<CharSequence, CharSequence>(CASE_INSENSITIVE_HASHER,
                                                                valueConverter(validate),
                                                                nameValidator));
    }

    protected DefaultHttpHeaders(DefaultHeaders<CharSequence, CharSequence, ?> headers) {
        this.headers = headers;
    }

    @Override
    public HttpHeaders add(HttpHeaders headers) {
        if (headers instanceof DefaultHttpHeaders) {
            this.headers.add(((DefaultHttpHeaders) headers).headers);
            return this;
        } else {
            return super.add(headers);
        }
    }

    @Override
    public HttpHeaders set(HttpHeaders headers) {
        if (headers instanceof DefaultHttpHeaders) {
            this.headers.set(((DefaultHttpHeaders) headers).headers);
            return this;
        } else {
            return super.set(headers);
        }
    }

    @Override
    public HttpHeaders add(String name, Object value) {
        headers.addObject(name, value);
        return this;
    }

    @Override
    public HttpHeaders add(CharSequence name, Object value) {
        headers.addObject(name, value);
        return this;
    }

    @Override
    public HttpHeaders add(String name, Iterable<?> values) {
        headers.addObject(name, values);
        return this;
    }

    @Override
    public HttpHeaders add(CharSequence name, Iterable<?> values) {
        headers.addObject(name, values);
        return this;
    }

    @Override
    public HttpHeaders addInt(CharSequence name, int value) {
        headers.addInt(name, value);
        return this;
    }

    @Override
    public HttpHeaders addShort(CharSequence name, short value) {
        headers.addShort(name, value);
        return this;
    }

    @Override
    public HttpHeaders remove(String name) {
        headers.remove(name);
        return this;
    }

    @Override
    public HttpHeaders remove(CharSequence name) {
        headers.remove(name);
        return this;
    }

    @Override
    public HttpHeaders set(String name, Object value) {
        headers.setObject(name, value);
        return this;
    }

    @Override
    public HttpHeaders set(CharSequence name, Object value) {
        headers.setObject(name, value);
        return this;
    }

    @Override
    public HttpHeaders set(String name, Iterable<?> values) {
        headers.setObject(name, values);
        return this;
    }

    @Override
    public HttpHeaders set(CharSequence name, Iterable<?> values) {
        headers.setObject(name, values);
        return this;
    }

    @Override
    public HttpHeaders setInt(CharSequence name, int value) {
        headers.setInt(name, value);
        return this;
    }

    @Override
    public HttpHeaders setShort(CharSequence name, short value) {
        headers.setShort(name, value);
        return this;
    }

    @Override
    public HttpHeaders clear() {
        headers.clear();
        return this;
    }

    @Override
    public String get(String name) {
        return get((CharSequence) name);
    }

    @Override
    public String get(CharSequence name) {
        return HeadersUtils.getAsString(headers, name);
    }

    @Override
    public Integer getInt(CharSequence name) {
        return headers.getInt(name);
    }

    @Override
    public int getInt(CharSequence name, int defaultValue) {
        return headers.getInt(name, defaultValue);
    }

    @Override
    public Short getShort(CharSequence name) {
        return headers.getShort(name);
    }

    @Override
    public short getShort(CharSequence name, short defaultValue) {
        return headers.getShort(name, defaultValue);
    }

    @Override
    public Long getTimeMillis(CharSequence name) {
        return headers.getTimeMillis(name);
    }

    @Override
    public long getTimeMillis(CharSequence name, long defaultValue) {
        return headers.getTimeMillis(name, defaultValue);
    }

    @Override
    public List<String> getAll(String name) {
        return getAll((CharSequence) name);
    }

    @Override
    public List<String> getAll(CharSequence name) {
        return HeadersUtils.getAllAsString(headers, name);
    }

    @Override
    public List<Entry<String, String>> entries() {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        List<Entry<String, String>> entriesConverted = new ArrayList<Entry<String, String>>(
                headers.size());
        for (Entry<String, String> entry : this) {
            entriesConverted.add(entry);
        }
        return entriesConverted;
    }

    @Deprecated
    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return HeadersUtils.iteratorAsString(headers);
    }

    @Override
    public Iterator<Entry<CharSequence, CharSequence>> iteratorCharSequence() {
        return headers.iterator();
    }

    @Override
    public boolean contains(String name) {
        return contains((CharSequence) name);
    }

    @Override
    public boolean contains(CharSequence name) {
        return headers.contains(name);
    }

    @Override
    public boolean isEmpty() {
        return headers.isEmpty();
    }

    @Override
    public int size() {
        return headers.size();
    }

    @Override
    public boolean contains(String name, String value, boolean ignoreCase) {
        return contains((CharSequence) name, (CharSequence) value, ignoreCase);
    }

    @Override
    public boolean contains(CharSequence name, CharSequence value, boolean ignoreCase) {
        return headers.contains(name, value, ignoreCase ? CASE_INSENSITIVE_HASHER : CASE_SENSITIVE_HASHER);
    }

    @Override
    public Set<String> names() {
        return HeadersUtils.namesAsString(headers);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DefaultHttpHeaders
                && headers.equals(((DefaultHttpHeaders) o).headers, CASE_SENSITIVE_HASHER);
    }

    @Override
    public int hashCode() {
        return headers.hashCode(CASE_SENSITIVE_HASHER);
    }

    private static void validateHeaderNameElement(byte value) {
        switch (value) {
        case 0x00:
        case '\t':
        case '\n':
        case 0x0b:
        case '\f':
        case '\r':
        case ' ':
        case ',':
        case ':':
        case ';':
        case '=':
            throw new IllegalArgumentException(
               "a header name cannot contain the following prohibited characters: =,;: \\t\\r\\n\\v\\f: " +
                       value);
        default:
            // Check to see if the character is not an ASCII character, or invalid
            if (value < 0) {
                throw new IllegalArgumentException("a header name cannot contain non-ASCII character: " +
                        value);
            }
        }
    }

    private static void validateHeaderNameElement(char value) {
        switch (value) {
        case 0x00:
        case '\t':
        case '\n':
        case 0x0b:
        case '\f':
        case '\r':
        case ' ':
        case ',':
        case ':':
        case ';':
        case '=':
            throw new IllegalArgumentException(
               "a header name cannot contain the following prohibited characters: =,;: \\t\\r\\n\\v\\f: " +
                       value);
        default:
            // Check to see if the character is not an ASCII character, or invalid
            if (value > 127) {
                throw new IllegalArgumentException("a header name cannot contain non-ASCII character: " +
                        value);
            }
        }
    }

    static ValueConverter<CharSequence> valueConverter(boolean validate) {
        return validate ? HeaderValueConverterAndValidator.INSTANCE : HeaderValueConverter.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    static NameValidator<CharSequence> nameValidator(boolean validate) {
        return validate ? HttpNameValidator : NameValidator.NOT_NULL;
    }

    private static class HeaderValueConverter extends CharSequenceValueConverter {
        static final HeaderValueConverter INSTANCE = new HeaderValueConverter();

        @Override
        public CharSequence convertObject(Object value) {
            if (value instanceof CharSequence) {
                return (CharSequence) value;
            }
            if (value instanceof Date) {
                return DateFormatter.format((Date) value);
            }
            if (value instanceof Calendar) {
                return DateFormatter.format(((Calendar) value).getTime());
            }
            return value.toString();
        }
    }

    private static final class HeaderValueConverterAndValidator extends HeaderValueConverter {
        static final HeaderValueConverterAndValidator INSTANCE = new HeaderValueConverterAndValidator();

        @Override
        public CharSequence convertObject(Object value) {
            CharSequence seq = super.convertObject(value);
            int state = 0;
            // Start looping through each of the character
            for (int index = 0; index < seq.length(); index++) {
                state = validateValueChar(seq, state, seq.charAt(index));
            }

            if (state != 0) {
                throw new IllegalArgumentException("a header value must not end with '\\r' or '\\n':" + seq);
            }
            return seq;
        }

        private static int validateValueChar(CharSequence seq, int state, char character) {
            /*
             * State:
             * 0: Previous character was neither CR nor LF
             * 1: The previous character was CR
             * 2: The previous character was LF
             */
            if ((character & HIGHEST_INVALID_VALUE_CHAR_MASK) == 0) {
                // Check the absolutely prohibited characters.
                switch (character) {
                case 0x0: // NULL
                    throw new IllegalArgumentException("a header value contains a prohibited character '\0': " + seq);
                case 0x0b: // Vertical tab
                    throw new IllegalArgumentException("a header value contains a prohibited character '\\v': " + seq);
                case '\f':
                    throw new IllegalArgumentException("a header value contains a prohibited character '\\f': " + seq);
                }
            }

            // Check the CRLF (HT | SP) pattern
            switch (state) {
                case 0:
                    switch (character) {
                        case '\r':
                            return 1;
                        case '\n':
                            return 2;
                    }
                    break;
                case 1:
                    switch (character) {
                        case '\n':
                            return 2;
                        default:
                            throw new IllegalArgumentException("only '\\n' is allowed after '\\r': " + seq);
                    }
                case 2:
                    switch (character) {
                        case '\t':
                        case ' ':
                            return 0;
                        default:
                            throw new IllegalArgumentException("only ' ' and '\\t' are allowed after '\\n': " + seq);
                    }
            }
            return state;
        }
    }
}
