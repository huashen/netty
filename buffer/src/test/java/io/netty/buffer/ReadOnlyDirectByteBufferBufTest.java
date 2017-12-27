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
package io.netty.buffer;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

public class ReadOnlyDirectByteBufferBufTest {

    protected ByteBuf buffer(ByteBuffer buffer) {
        return new ReadOnlyByteBufferBuf(UnpooledByteBufAllocator.DEFAULT, buffer);
    }

    protected ByteBuffer allocate(int size) {
        return ByteBuffer.allocateDirect(size);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructWithWritable() {
        buffer(allocate(1));
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetByte() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setByte(0, 1);
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetInt() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setInt(0, 1);
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetShort() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setShort(0, 1);
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetMedium() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setMedium(0, 1);
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetLong() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setLong(0, 1);
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetBytesViaArray() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        try {
            buf.setBytes(0, "test".getBytes());
        } finally {
            buf.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetBytesViaBuffer() {
        ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
        ByteBuf copy = Unpooled.copyInt(1);
        try {
            buf.setBytes(0, copy);
        } finally {
            buf.release();
            copy.release();
        }
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetBytesViaStream() throws IOException {
        ByteBuf buf = buffer(ByteBuffer.allocateDirect(8).asReadOnlyBuffer());
        try {
            buf.setBytes(0, new ByteArrayInputStream("test".getBytes()), 2);
        } finally {
            buf.release();
        }
    }

    @Test
    public void testGetReadByte() {
        ByteBuf buf = buffer(
                ((ByteBuffer) allocate(2).put(new byte[] { (byte) 1, (byte) 2 }).flip()).asReadOnlyBuffer());

        Assert.assertEquals(1, buf.getByte(0));
        Assert.assertEquals(2, buf.getByte(1));

        Assert.assertEquals(1, buf.readByte());
        Assert.assertEquals(2, buf.readByte());
        Assert.assertFalse(buf.isReadable());

        buf.release();
    }

    @Test
    public void testGetReadInt() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(8).putInt(1).putInt(2).flip()).asReadOnlyBuffer());

        Assert.assertEquals(1, buf.getInt(0));
        Assert.assertEquals(2, buf.getInt(4));

        Assert.assertEquals(1, buf.readInt());
        Assert.assertEquals(2, buf.readInt());
        Assert.assertFalse(buf.isReadable());

        buf.release();
    }

    @Test
    public void testGetReadShort() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(8)
                .putShort((short) 1).putShort((short) 2).flip()).asReadOnlyBuffer());

        Assert.assertEquals(1, buf.getShort(0));
        Assert.assertEquals(2, buf.getShort(2));

        Assert.assertEquals(1, buf.readShort());
        Assert.assertEquals(2, buf.readShort());
        Assert.assertFalse(buf.isReadable());

        buf.release();
    }

    @Test
    public void testGetReadLong() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(16)
                .putLong(1).putLong(2).flip()).asReadOnlyBuffer());

        Assert.assertEquals(1, buf.getLong(0));
        Assert.assertEquals(2, buf.getLong(8));

        Assert.assertEquals(1, buf.readLong());
        Assert.assertEquals(2, buf.readLong());
        Assert.assertFalse(buf.isReadable());

        buf.release();
    }

    @Test
    public void testCopy() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).putLong(2).flip()).asReadOnlyBuffer());
        ByteBuf copy = buf.copy();

        Assert.assertEquals(buf, copy);

        buf.release();
        copy.release();
    }

    @Test
    public void testCopyWithOffset() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).putLong(2).flip()).asReadOnlyBuffer());
        ByteBuf copy = buf.copy(1, 9);

        Assert.assertEquals(buf.slice(1, 9), copy);

        buf.release();
        copy.release();
    }

    // Test for https://github.com/netty/netty/issues/1708
    @Test
    public void testWrapBufferWithNonZeroPosition() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(16)
                .putLong(1).flip().position(1)).asReadOnlyBuffer());

        ByteBuf slice = buf.slice();
        Assert.assertEquals(buf, slice);

        buf.release();
    }

    @Test
    public void testWrapBufferRoundTrip() {
        ByteBuf buf = buffer(((ByteBuffer) allocate(16).putInt(1).putInt(2).flip()).asReadOnlyBuffer());

        Assert.assertEquals(1, buf.readInt());

        ByteBuffer nioBuffer = buf.nioBuffer();

        // Ensure this can be accessed without throwing a BufferUnderflowException
        Assert.assertEquals(2, nioBuffer.getInt());

        buf.release();
    }
}
