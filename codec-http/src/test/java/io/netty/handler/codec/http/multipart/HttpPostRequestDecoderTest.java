/*
 * Copyright 2013 The Netty Project
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
package io.netty.handler.codec.http.multipart;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.junit.Test;

import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@link HttpPostRequestDecoder} test case. */
public class HttpPostRequestDecoderTest {

    @Test
    public void testBinaryStreamUploadWithSpace() throws Exception {
        testBinaryStreamUpload(true);
    }

    // https://github.com/netty/netty/issues/1575
    @Test
    public void testBinaryStreamUploadWithoutSpace() throws Exception {
        testBinaryStreamUpload(false);
    }

    private static void testBinaryStreamUpload(boolean withSpace) throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        final String contentTypeValue;
        if (withSpace) {
            contentTypeValue = "multipart/form-data; boundary=" + boundary;
        } else {
            contentTypeValue = "multipart/form-data;boundary=" + boundary;
        }
        final DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "http://localhost");

        req.setDecoderResult(DecoderResult.SUCCESS);
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, contentTypeValue);
        req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);

        for (String data : Arrays.asList("", "\r", "\r\r", "\r\r\r")) {
            final String body =
                    "--" + boundary + "\r\n" +
                    "Content-Disposition: form-data; name=\"file\"; filename=\"tmp-0.txt\"\r\n" +
                    "Content-Type: image/gif\r\n" +
                    "\r\n" +
                    data + "\r\n" +
                    "--" + boundary + "--\r\n";

            // Create decoder instance to test.
            final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);

            decoder.offer(new DefaultHttpContent(Unpooled.copiedBuffer(body, CharsetUtil.UTF_8)));
            decoder.offer(new DefaultHttpContent(Unpooled.EMPTY_BUFFER));

            // Validate it's enough chunks to decode upload.
            assertTrue(decoder.hasNext());

            // Decode binary upload.
            MemoryFileUpload upload = (MemoryFileUpload) decoder.next();

            // Validate data has been parsed correctly as it was passed into request.
            assertEquals("Invalid decoded data [data=" + data.replaceAll("\r", "\\\\r") + ", upload=" + upload + ']',
                data, upload.getString(CharsetUtil.UTF_8));
            upload.release();
            decoder.destroy();
        }
    }

    // See https://github.com/netty/netty/issues/1089
    @Test
    public void testFullHttpRequestUpload() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";

        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "http://localhost");

        req.setDecoderResult(DecoderResult.SUCCESS);
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);

        for (String data : Arrays.asList("", "\r", "\r\r", "\r\r\r")) {
            final String body =
                    "--" + boundary + "\r\n" +
                            "Content-Disposition: form-data; name=\"file\"; filename=\"tmp-0.txt\"\r\n" +
                            "Content-Type: image/gif\r\n" +
                            "\r\n" +
                            data + "\r\n" +
                            "--" + boundary + "--\r\n";

            req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
        }
        // Create decoder instance to test.
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
        assertFalse(decoder.getBodyHttpDatas().isEmpty());
        decoder.destroy();
    }

    // See https://github.com/netty/netty/issues/2544
    @Test
    public void testMultipartCodecWithCRasEndOfAttribute() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";

        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);
        // Build test case
        String extradata = "aaaa";
        String[] datas = new String[5];
        for (int i = 0; i < 4; i++) {
            datas[i] = extradata;
            for (int j = 0; j < i; j++) {
                datas[i] += '\r';
            }
        }

        for (int i = 0; i < 4; i++) {
            final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                    "http://localhost");
            req.setDecoderResult(DecoderResult.SUCCESS);
            req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
            req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
            final String body =
                    "--" + boundary + "\r\n" +
                            "Content-Disposition: form-data; name=\"file" + i + "\"\r\n" +
                            "Content-Type: image/gif\r\n" +
                            "\r\n" +
                            datas[i] + "\r\n" +
                            "--" + boundary + "--\r\n";

            req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
            // Create decoder instance to test.
            final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
            assertFalse(decoder.getBodyHttpDatas().isEmpty());
            // Check correctness: data size
            InterfaceHttpData httpdata = decoder.getBodyHttpData("file" + i);
            assertNotNull(httpdata);
            Attribute attribute = (Attribute) httpdata;
            byte[] datar = attribute.get();
            assertNotNull(datar);
            assertEquals(datas[i].getBytes(CharsetUtil.UTF_8).length, datar.length);

            decoder.destroy();
        }
    }

    // See https://github.com/netty/netty/issues/2542
    @Test
    public void testQuotedBoundary() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";

        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "http://localhost");

        req.setDecoderResult(DecoderResult.SUCCESS);
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=\"" + boundary + '"');
        req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);

        for (String data : Arrays.asList("", "\r", "\r\r", "\r\r\r")) {
            final String body =
                    "--" + boundary + "\r\n" +
                            "Content-Disposition: form-data; name=\"file\"; filename=\"tmp-0.txt\"\r\n" +
                            "Content-Type: image/gif\r\n" +
                            "\r\n" +
                            data + "\r\n" +
                            "--" + boundary + "--\r\n";

            req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
        }
        // Create decoder instance to test.
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
        assertFalse(decoder.getBodyHttpDatas().isEmpty());
        decoder.destroy();
    }

    // See https://github.com/netty/netty/issues/1848
    @Test
    public void testNoZeroOut() throws Exception {
        final String boundary = "E832jQp_Rq2ErFmAduHSR8YlMSm0FCY";

        final DefaultHttpDataFactory aMemFactory = new DefaultHttpDataFactory(false);

        DefaultHttpRequest aRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                             HttpMethod.POST,
                                                             "http://localhost");
        aRequest.headers().set(HttpHeaderNames.CONTENT_TYPE,
                               "multipart/form-data; boundary=" + boundary);
        aRequest.headers().set(HttpHeaderNames.TRANSFER_ENCODING,
                               HttpHeaderValues.CHUNKED);

        HttpPostRequestDecoder aDecoder = new HttpPostRequestDecoder(aMemFactory, aRequest);

        final String aData = "some data would be here. the data should be long enough that it " +
                             "will be longer than the original buffer length of 256 bytes in " +
                             "the HttpPostRequestDecoder in order to trigger the issue. Some more " +
                             "data just to be on the safe side.";

        final String body =
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"root\"\r\n" +
                "Content-Type: text/plain\r\n" +
                "\r\n" +
                aData +
                "\r\n" +
                "--" + boundary + "--\r\n";

        byte[] aBytes = body.getBytes();

        int split = 125;

        ByteBufAllocator aAlloc = new UnpooledByteBufAllocator(true);
        ByteBuf aSmallBuf = aAlloc.heapBuffer(split, split);
        ByteBuf aLargeBuf = aAlloc.heapBuffer(aBytes.length - split, aBytes.length - split);

        aSmallBuf.writeBytes(aBytes, 0, split);
        aLargeBuf.writeBytes(aBytes, split, aBytes.length - split);

        aDecoder.offer(new DefaultHttpContent(aSmallBuf));
        aDecoder.offer(new DefaultHttpContent(aLargeBuf));

        aDecoder.offer(LastHttpContent.EMPTY_LAST_CONTENT);

        assertTrue("Should have a piece of data", aDecoder.hasNext());

        InterfaceHttpData aDecodedData = aDecoder.next();
        assertEquals(InterfaceHttpData.HttpDataType.Attribute, aDecodedData.getHttpDataType());

        Attribute aAttr = (Attribute) aDecodedData;
        assertEquals(aData, aAttr.getValue());

        aDecodedData.release();
        aDecoder.destroy();
    }

    // See https://github.com/netty/netty/issues/2305
    @Test
    public void testChunkCorrect() throws Exception {
       String payload = "town=794649819&town=784444184&town=794649672&town=794657800&town=" +
                "794655734&town=794649377&town=794652136&town=789936338&town=789948986&town=" +
                "789949643&town=786358677&town=794655880&town=786398977&town=789901165&town=" +
                "789913325&town=789903418&town=789903579&town=794645251&town=794694126&town=" +
                "794694831&town=794655274&town=789913656&town=794653956&town=794665634&town=" +
                "789936598&town=789904658&town=789899210&town=799696252&town=794657521&town=" +
                "789904837&town=789961286&town=789958704&town=789948839&town=789933899&town=" +
                "793060398&town=794659180&town=794659365&town=799724096&town=794696332&town=" +
                "789953438&town=786398499&town=794693372&town=789935439&town=794658041&town=" +
                "789917595&town=794655427&town=791930372&town=794652891&town=794656365&town=" +
                "789960339&town=794645586&town=794657688&town=794697211&town=789937427&town=" +
                "789902813&town=789941130&town=794696907&town=789904328&town=789955151&town=" +
                "789911570&town=794655074&town=789939531&town=789935242&town=789903835&town=" +
                "789953800&town=794649962&town=789939841&town=789934819&town=789959672&town=" +
                "794659043&town=794657035&town=794658938&town=794651746&town=794653732&town=" +
                "794653881&town=786397909&town=794695736&town=799724044&town=794695926&town=" +
                "789912270&town=794649030&town=794657946&town=794655370&town=794659660&town=" +
                "794694617&town=799149862&town=789953234&town=789900476&town=794654995&town=" +
                "794671126&town=789908868&town=794652942&town=789955605&town=789901934&town=" +
                "789950015&town=789937922&town=789962576&town=786360170&town=789954264&town=" +
                "789911738&town=789955416&town=799724187&town=789911879&town=794657462&town=" +
                "789912561&town=789913167&town=794655195&town=789938266&town=789952099&town=" +
                "794657160&town=789949414&town=794691293&town=794698153&town=789935636&town=" +
                "789956374&town=789934635&town=789935475&town=789935085&town=794651425&town=" +
                "794654936&town=794655680&town=789908669&town=794652031&town=789951298&town=" +
                "789938382&town=794651503&town=794653330&town=817675037&town=789951623&town=" +
                "789958999&town=789961555&town=794694050&town=794650241&town=794656286&town=" +
                "794692081&town=794660090&town=794665227&town=794665136&town=794669931";
        DefaultHttpRequest defaultHttpRequest =
                    new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");

        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(defaultHttpRequest);

        int firstChunk = 10;
        int middleChunk = 1024;

        HttpContent part1 = new DefaultHttpContent(Unpooled.wrappedBuffer(
                payload.substring(0, firstChunk).getBytes()));
        HttpContent part2 = new DefaultHttpContent(Unpooled.wrappedBuffer(
                payload.substring(firstChunk, firstChunk + middleChunk).getBytes()));
        HttpContent part3 = new DefaultHttpContent(Unpooled.wrappedBuffer(
                payload.substring(firstChunk + middleChunk, firstChunk + middleChunk * 2).getBytes()));
        HttpContent part4 = new DefaultHttpContent(Unpooled.wrappedBuffer(
                payload.substring(firstChunk + middleChunk * 2).getBytes()));

        decoder.offer(part1);
        decoder.offer(part2);
        decoder.offer(part3);
        decoder.offer(part4);
    }

    // See https://github.com/netty/netty/issues/3326
    @Test
    public void testFilenameContainingSemicolon() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "http://localhost");
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);
        final String data = "asdf";
        final String filename = "tmp;0.txt";
        final String body =
                "--" + boundary + "\r\n" +
                        "Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n" +
                        "Content-Type: image/gif\r\n" +
                        "\r\n" +
                        data + "\r\n" +
                        "--" + boundary + "--\r\n";

        req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8.name()));
        // Create decoder instance to test.
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
        assertFalse(decoder.getBodyHttpDatas().isEmpty());
        decoder.destroy();
    }

    @Test
    public void testFilenameContainingSemicolon2() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                                                                      "http://localhost");
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);
        final String data = "asdf";
        final String filename = "tmp;0.txt";
        final String body =
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n" +
                "Content-Type: image/gif\r\n" +
                "\r\n" +
                data + "\r\n" +
                "--" + boundary + "--\r\n";

        req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8.name()));
        // Create decoder instance to test.
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
        assertFalse(decoder.getBodyHttpDatas().isEmpty());
        InterfaceHttpData part1 = decoder.getBodyHttpDatas().get(0);
        assertTrue(part1 instanceof FileUpload);
        FileUpload fileUpload = (FileUpload) part1;
        assertEquals("tmp 0.txt", fileUpload.getFilename());
        decoder.destroy();
    }

    @Test
    public void testMultipartRequestWithoutContentTypeBody() {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";

        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "http://localhost");

        req.setDecoderResult(DecoderResult.SUCCESS);
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        req.headers().add(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);

        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);

        for (String data : Arrays.asList("", "\r", "\r\r", "\r\r\r")) {
            final String body =
                    "--" + boundary + "\r\n" +
                            "Content-Disposition: form-data; name=\"file\"; filename=\"tmp-0.txt\"\r\n" +
                            "\r\n" +
                            data + "\r\n" +
                            "--" + boundary + "--\r\n";

            req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
        }
        // Create decoder instance to test without any exception.
        final HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(inMemoryFactory, req);
        assertFalse(decoder.getBodyHttpDatas().isEmpty());
        decoder.destroy();
    }

    @Test
    public void testMultipartRequestWithFileInvalidCharset() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            "http://localhost");
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);
        final String data = "asdf";
        final String filename = "tmp;0.txt";
        final String body =
            "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n" +
                "Content-Type: image/gif; charset=ABCD\r\n" +
                "\r\n" +
                data + "\r\n" +
                "--" + boundary + "--\r\n";

        req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
        // Create decoder instance to test.
        try {
            new HttpPostRequestDecoder(inMemoryFactory, req);
            fail("Was expecting an ErrorDataDecoderException");
        } catch (HttpPostRequestDecoder.ErrorDataDecoderException e) {
            assertTrue(e.getCause() instanceof UnsupportedCharsetException);
        } finally {
            req.release();
        }
    }

    @Test
    public void testMultipartRequestWithFieldInvalidCharset() throws Exception {
        final String boundary = "dLV9Wyq26L_-JQxk6ferf-RT153LhOO";
        final DefaultFullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            "http://localhost");
        req.headers().add(HttpHeaderNames.CONTENT_TYPE, "multipart/form-data; boundary=" + boundary);
        // Force to use memory-based data.
        final DefaultHttpDataFactory inMemoryFactory = new DefaultHttpDataFactory(false);
        final String aData = "some data would be here. the data should be long enough that it " +
            "will be longer than the original buffer length of 256 bytes in " +
            "the HttpPostRequestDecoder in order to trigger the issue. Some more " +
            "data just to be on the safe side.";
        final String body =
            "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"root\"\r\n" +
                "Content-Type: text/plain; charset=ABCD\r\n" +
                "\r\n" +
                aData +
                "\r\n" +
                "--" + boundary + "--\r\n";

        req.content().writeBytes(body.getBytes(CharsetUtil.UTF_8));
        // Create decoder instance to test.
        try {
            new HttpPostRequestDecoder(inMemoryFactory, req);
            fail("Was expecting an ErrorDataDecoderException");
        } catch (HttpPostRequestDecoder.ErrorDataDecoderException e) {
            assertTrue(e.getCause() instanceof UnsupportedCharsetException);
        } finally {
            req.release();
        }
    }

    @Test
    public void testFormEncodeIncorrect() throws Exception {
        LastHttpContent content = new DefaultLastHttpContent(
                Unpooled.copiedBuffer("project=netty&&project=netty", CharsetUtil.US_ASCII));
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(req);
        try {
            decoder.offer(content);
            fail();
        } catch (HttpPostRequestDecoder.ErrorDataDecoderException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        } finally {
            decoder.destroy();
            content.release();
        }
    }
}
