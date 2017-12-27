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
package io.netty.handler.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.NetUtil;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class HttpProxyHandlerTest {

    @Test
    public void testIpv6() throws Exception  {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName("::1"), 8080);
        testInitialMessage(socketAddress, "[::1]:8080", null);
    }

    @Test
    public void testIpv6Unresolved() throws Exception  {
        InetSocketAddress socketAddress = InetSocketAddress.createUnresolved("::1", 8080);
        testInitialMessage(socketAddress, "[::1]:8080", null);
    }

    @Test
    public void testIpv4() throws Exception  {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getByName("10.0.0.1"), 8080);
        testInitialMessage(socketAddress, "10.0.0.1:8080", null);
    }

    @Test
    public void testIpv4Unresolved() throws Exception  {
        InetSocketAddress socketAddress = InetSocketAddress.createUnresolved("10.0.0.1", 8080);
        testInitialMessage(socketAddress, "10.0.0.1:8080", null);
    }

    @Test
    public void testCustomHeaders() throws Exception {
        InetSocketAddress socketAddress = InetSocketAddress.createUnresolved("10.0.0.1", 8080);
        testInitialMessage(socketAddress, "10.0.0.1:8080",
                           new DefaultHttpHeaders().add("CUSTOM_HEADER", "CUSTOM_VALUE1")
                                                   .add("CUSTOM_HEADER", "CUSTOM_VALUE2"));
    }

    private static void testInitialMessage(InetSocketAddress socketAddress, String hostPort,
                                           HttpHeaders headers) throws Exception  {
        InetSocketAddress proxyAddress = new InetSocketAddress(NetUtil.LOCALHOST, 8080);

        ChannelPromise promise = mock(ChannelPromise.class);
        verifyNoMoreInteractions(promise);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.connect(same(proxyAddress), isNull(InetSocketAddress.class), same(promise))).thenReturn(promise);

        HttpProxyHandler handler = new HttpProxyHandler(new InetSocketAddress(NetUtil.LOCALHOST, 8080), headers);
        handler.connect(ctx, socketAddress, null, promise);

        FullHttpRequest request = (FullHttpRequest) handler.newInitialMessage(ctx);
        try {
            assertEquals(HttpVersion.HTTP_1_1, request.protocolVersion());
            assertEquals(hostPort, request.uri());
            HttpHeaders actualHeaders = request.headers();
            assertEquals(hostPort, actualHeaders.get(HttpHeaderNames.HOST));

            if (headers != null) {
                // The actual request header is a strict superset of the custom header
                for (String name : headers.names()) {
                    assertEquals(headers.getAll(name), actualHeaders.getAll(name));
                }
            }
        } finally {
            request.release();
        }
        verify(ctx).connect(proxyAddress, null, promise);
    }
}
