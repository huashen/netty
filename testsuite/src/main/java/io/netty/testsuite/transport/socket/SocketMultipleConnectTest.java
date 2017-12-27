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
package io.netty.testsuite.transport.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.testsuite.transport.TestsuitePermutation;
import io.netty.util.NetUtil;
import org.junit.Test;

import java.nio.channels.AlreadyConnectedException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SocketMultipleConnectTest extends AbstractSocketTest {

    @Test(timeout = 30000)
    public void testMultipleConnect() throws Throwable {
        run();
    }

    public void testMultipleConnect(ServerBootstrap sb, Bootstrap cb) throws Exception {
        Channel sc = null;
        Channel cc = null;
        try {
            sb.childHandler(new ChannelInboundHandlerAdapter());
            sc = sb.bind(NetUtil.LOCALHOST, 0).syncUninterruptibly().channel();

            cb.handler(new ChannelInboundHandlerAdapter());
            cc = cb.register().syncUninterruptibly().channel();
            cc.connect(sc.localAddress()).syncUninterruptibly();
            ChannelFuture connectFuture2 = cc.connect(sc.localAddress()).await();
            assertTrue(connectFuture2.cause() instanceof AlreadyConnectedException);
        } finally {
            if (cc != null) {
                cc.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
    }

    @Override
    protected List<TestsuitePermutation.BootstrapComboFactory<ServerBootstrap, Bootstrap>> newFactories() {
        List<TestsuitePermutation.BootstrapComboFactory<ServerBootstrap, Bootstrap>> factories
                = new ArrayList<TestsuitePermutation.BootstrapComboFactory<ServerBootstrap, Bootstrap>>();
        for (TestsuitePermutation.BootstrapComboFactory<ServerBootstrap, Bootstrap> comboFactory
                : SocketTestPermutation.INSTANCE.socket()) {
            if (comboFactory.newClientInstance().config().group() instanceof NioEventLoopGroup) {
                factories.add(comboFactory);
            }
        }
        return factories;
    }
}
