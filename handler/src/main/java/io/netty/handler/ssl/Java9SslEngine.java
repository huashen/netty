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

import io.netty.util.internal.StringUtil;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiFunction;

import static io.netty.handler.ssl.SslUtils.toSSLHandshakeException;
import static io.netty.handler.ssl.JdkApplicationProtocolNegotiator.ProtocolSelectionListener;
import static io.netty.handler.ssl.JdkApplicationProtocolNegotiator.ProtocolSelector;

final class Java9SslEngine extends JdkSslEngine {
    private final ProtocolSelectionListener selectionListener;
    private final AlpnSelector alpnSelector;

    private final class AlpnSelector implements BiFunction<SSLEngine, List<String>, String> {
        private final ProtocolSelector selector;
        private boolean called;

        AlpnSelector(ProtocolSelector selector) {
            this.selector = selector;
        }

        @Override
        public String apply(SSLEngine sslEngine, List<String> strings) {
            assert !called;
            called = true;

            try {
                String selected = selector.select(strings);
                return selected == null ? StringUtil.EMPTY_STRING : selected;
            } catch (Exception cause) {
                // Returning null means we want to fail the handshake.
                //
                // See http://download.java.net/java/jdk9/docs/api/javax/net/ssl/
                // SSLEngine.html#setHandshakeApplicationProtocolSelector-java.util.function.BiFunction-
                return null;
            }
        }

        void checkUnsupported() {
            if (called) {
                // ALPN message was received by peer and so apply(...) was called.
                // See:
                // http://hg.openjdk.java.net/jdk9/dev/jdk/file/65464a307408/src/
                // java.base/share/classes/sun/security/ssl/ServerHandshaker.java#l933
                return;
            }
            String protocol = getApplicationProtocol();
            assert protocol != null;

            if (protocol.isEmpty()) {
                // ALPN is not supported
                selector.unsupported();
            }
        }
    }

    Java9SslEngine(SSLEngine engine, JdkApplicationProtocolNegotiator applicationNegotiator, boolean isServer) {
        super(engine);
        if (isServer) {
            selectionListener = null;
            alpnSelector = new AlpnSelector(applicationNegotiator.protocolSelectorFactory().
                    newSelector(this, new LinkedHashSet<String>(applicationNegotiator.protocols())));
            Java9SslUtils.setHandshakeApplicationProtocolSelector(engine, alpnSelector);
        } else {
            selectionListener = applicationNegotiator.protocolListenerFactory()
                    .newListener(this, applicationNegotiator.protocols());
            alpnSelector = null;
            Java9SslUtils.setApplicationProtocols(engine, applicationNegotiator.protocols());
        }
    }

    private SSLEngineResult verifyProtocolSelection(SSLEngineResult result) throws SSLException {
        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            if (alpnSelector == null) {
                // This means we are using client-side and
                try {
                    String protocol = getApplicationProtocol();
                    assert protocol != null;
                    if (protocol.isEmpty()) {
                        // If empty the server did not announce ALPN:
                        // See:
                        // http://hg.openjdk.java.net/jdk9/dev/jdk/file/65464a307408/src/java.base/
                        // share/classes/sun/security/ssl/ClientHandshaker.java#l741
                        selectionListener.unsupported();
                    } else {
                        selectionListener.selected(protocol);
                    }
                } catch (Throwable e) {
                    throw toSSLHandshakeException(e);
                }
            } else {
                assert selectionListener == null;
                alpnSelector.checkUnsupported();
            }
        }
        return result;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return verifyProtocolSelection(super.wrap(src, dst));
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, ByteBuffer dst) throws SSLException {
        return verifyProtocolSelection(super.wrap(srcs, dst));
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int len, ByteBuffer dst) throws SSLException {
        return verifyProtocolSelection(super.wrap(srcs, offset, len, dst));
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
        return verifyProtocolSelection(super.unwrap(src, dst));
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts) throws SSLException {
        return verifyProtocolSelection(super.unwrap(src, dsts));
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dst, int offset, int len) throws SSLException {
        return verifyProtocolSelection(super.unwrap(src, dst, offset, len));
    }

    @Override
    void setApplicationProtocol(String applicationProtocol) {
        // Do nothing as this is handled internally by the Java9 implementation of SSLEngine.
    }

    @Override
    public String getApplicationProtocol() {
        return Java9SslUtils.getApplicationProtocol(getWrappedEngine());
    }

    // These methods will override the methods defined by Java 9. As we compile with Java8 we can not add
    // @Override annotations here.
    public String getHandshakeApplicationProtocol() {
        return Java9SslUtils.getHandshakeApplicationProtocol(getWrappedEngine());
    }

    public void setHandshakeApplicationProtocolSelector(BiFunction<SSLEngine, List<String>, String> selector) {
        Java9SslUtils.setHandshakeApplicationProtocolSelector(getWrappedEngine(), selector);
    }

    public BiFunction<SSLEngine, List<String>, String> getHandshakeApplicationProtocolSelector() {
        return Java9SslUtils.getHandshakeApplicationProtocolSelector(getWrappedEngine());
    }
}
