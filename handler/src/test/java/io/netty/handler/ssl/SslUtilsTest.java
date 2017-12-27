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
package io.netty.handler.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;

import static io.netty.handler.ssl.SslUtils.getEncryptedPacketLength;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SslUtilsTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testPacketLength() throws SSLException, NoSuchAlgorithmException {
        SSLEngine engineLE = newEngine();
        SSLEngine engineBE = newEngine();

        ByteBuffer empty = ByteBuffer.allocate(0);
        ByteBuffer cTOsLE = ByteBuffer.allocate(17 * 1024).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer cTOsBE = ByteBuffer.allocate(17 * 1024);

        assertTrue(engineLE.wrap(empty, cTOsLE).bytesProduced() > 0);
        cTOsLE.flip();

        assertTrue(engineBE.wrap(empty, cTOsBE).bytesProduced() > 0);
        cTOsBE.flip();

        ByteBuf bufferLE = Unpooled.buffer().order(ByteOrder.LITTLE_ENDIAN).writeBytes(cTOsLE);
        ByteBuf bufferBE = Unpooled.buffer().writeBytes(cTOsBE);

        // Test that the packet-length for BE and LE is the same
        assertEquals(getEncryptedPacketLength(bufferBE, 0), getEncryptedPacketLength(bufferLE, 0));
        assertEquals(getEncryptedPacketLength(new ByteBuffer[] { bufferBE.nioBuffer() }, 0),
                getEncryptedPacketLength(new ByteBuffer[] { bufferLE.nioBuffer().order(ByteOrder.LITTLE_ENDIAN) }, 0));
    }

    private static SSLEngine newEngine() throws SSLException, NoSuchAlgorithmException  {
        SSLEngine engine = SSLContext.getDefault().createSSLEngine();
        engine.setUseClientMode(true);
        engine.beginHandshake();
        return engine;
    }
}
