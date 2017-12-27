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
package io.netty.buffer;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractPooledByteBufTest extends AbstractByteBufTest {

    protected abstract ByteBuf alloc(int length, int maxCapacity);

    @Override
    protected ByteBuf newBuffer(int length, int maxCapacity) {
        ByteBuf buffer = alloc(length, maxCapacity);

        // Testing if the writerIndex and readerIndex are correct when allocate and also after we reset the mark.
        assertEquals(0, buffer.writerIndex());
        assertEquals(0, buffer.readerIndex());
        buffer.resetReaderIndex();
        buffer.resetWriterIndex();
        assertEquals(0, buffer.writerIndex());
        assertEquals(0, buffer.readerIndex());
        return buffer;
    }

    @Test
    public void ensureWritableWithEnoughSpaceShouldNotThrow() {
        ByteBuf buf = newBuffer(1, 10);
        buf.ensureWritable(3);
        assertThat(buf.writableBytes(), is(greaterThanOrEqualTo(3)));
        buf.release();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void ensureWritableWithNotEnoughSpaceShouldThrow() {
        ByteBuf buf = newBuffer(1, 10);
        try {
            buf.ensureWritable(11);
            fail();
        } finally {
            buf.release();
        }
    }
}
