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
package io.netty.handler.codec.string;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.Assert;
import org.junit.Test;

public class StringEncoderTest {

    @Test
    public void testEncode() {
        String msg = "Test";
        EmbeddedChannel channel = new EmbeddedChannel(new StringEncoder());
        Assert.assertTrue(channel.writeOutbound(msg));
        Assert.assertTrue(channel.finish());
        ByteBuf buf = channel.readOutbound();
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        Assert.assertArrayEquals(msg.getBytes(CharsetUtil.UTF_8), data);
        Assert.assertNull(channel.readOutbound());
        buf.release();
    }
}
