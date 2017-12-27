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
package io.netty.handler.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.internal.tcnative.CertificateVerifier;
import io.netty.internal.tcnative.SSL;
import io.netty.internal.tcnative.SSLContext;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.ResourceLeakTracker;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.security.AccessController;
import java.security.PrivateKey;
import java.security.PrivilegedAction;
import java.security.cert.CertPathValidatorException;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CertificateRevokedException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import static io.netty.handler.ssl.OpenSsl.DEFAULT_CIPHERS;
import static io.netty.handler.ssl.OpenSsl.availableJavaCipherSuites;
import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

/**
 * An implementation of {@link SslContext} which works with libraries that support the
 * <a href="https://www.openssl.org/">OpenSsl</a> C library API.
 * <p>Instances of this class must be {@link #release() released} or else native memory will leak!
 *
 * <p>Instances of this class <strong>must not</strong> be released before any {@link ReferenceCountedOpenSslEngine}
 * which depends upon the instance of this class is released. Otherwise if any method of
 * {@link ReferenceCountedOpenSslEngine} is called which uses this class's JNI resources the JVM may crash.
 */
public abstract class ReferenceCountedOpenSslContext extends SslContext implements ReferenceCounted {
    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(ReferenceCountedOpenSslContext.class);
    /**
     * To make it easier for users to replace JDK implementation with OpenSsl version we also use
     * {@code jdk.tls.rejectClientInitiatedRenegotiation} to allow disabling client initiated renegotiation.
     * Java8+ uses this system property as well.
     * <p>
     * See also <a href="http://blog.ivanristic.com/2014/03/ssl-tls-improvements-in-java-8.html">
     * Significant SSL/TLS improvements in Java 8</a>
     */
    private static final boolean JDK_REJECT_CLIENT_INITIATED_RENEGOTIATION =
            AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
                @Override
                public Boolean run() {
                    return SystemPropertyUtil.getBoolean("jdk.tls.rejectClientInitiatedRenegotiation", false);
                }
            });

    private static final int DEFAULT_BIO_NON_APPLICATION_BUFFER_SIZE =
            AccessController.doPrivileged(new PrivilegedAction<Integer>() {
                @Override
                public Integer run() {
                    return Math.max(1,
                            SystemPropertyUtil.getInt("io.netty.handler.ssl.openssl.bioNonApplicationBufferSize",
                                                      2048));
                }
            });

    private static final Integer DH_KEY_LENGTH;
    private static final ResourceLeakDetector<ReferenceCountedOpenSslContext> leakDetector =
            ResourceLeakDetectorFactory.instance().newResourceLeakDetector(ReferenceCountedOpenSslContext.class);

    // TODO: Maybe make configurable ?
    protected static final int VERIFY_DEPTH = 10;

    /**
     * The OpenSSL SSL_CTX object.
     *
     * <strong>{@link #ctxLock} must be hold while using ctx!</strong>
     */
    protected long ctx;
    private final List<String> unmodifiableCiphers;
    private final long sessionCacheSize;
    private final long sessionTimeout;
    private final OpenSslApplicationProtocolNegotiator apn;
    private final int mode;

    // Reference Counting
    private final ResourceLeakTracker<ReferenceCountedOpenSslContext> leak;
    private final AbstractReferenceCounted refCnt = new AbstractReferenceCounted() {
        @Override
        public ReferenceCounted touch(Object hint) {
            if (leak != null) {
                leak.record(hint);
            }

            return ReferenceCountedOpenSslContext.this;
        }

        @Override
        protected void deallocate() {
            destroy();
            if (leak != null) {
                boolean closed = leak.close(ReferenceCountedOpenSslContext.this);
                assert closed;
            }
        }
    };

    final Certificate[] keyCertChain;
    final ClientAuth clientAuth;
    final String[] protocols;
    final boolean enableOcsp;
    final OpenSslEngineMap engineMap = new DefaultOpenSslEngineMap();
    final ReadWriteLock ctxLock = new ReentrantReadWriteLock();

    private volatile boolean rejectRemoteInitiatedRenegotiation;
    private volatile int bioNonApplicationBufferSize = DEFAULT_BIO_NON_APPLICATION_BUFFER_SIZE;

    @SuppressWarnings("deprecation")
    static final OpenSslApplicationProtocolNegotiator NONE_PROTOCOL_NEGOTIATOR =
            new OpenSslApplicationProtocolNegotiator() {
                @Override
                public ApplicationProtocolConfig.Protocol protocol() {
                    return ApplicationProtocolConfig.Protocol.NONE;
                }

                @Override
                public List<String> protocols() {
                    return Collections.emptyList();
                }

                @Override
                public ApplicationProtocolConfig.SelectorFailureBehavior selectorFailureBehavior() {
                    return ApplicationProtocolConfig.SelectorFailureBehavior.CHOOSE_MY_LAST_PROTOCOL;
                }

                @Override
                public ApplicationProtocolConfig.SelectedListenerFailureBehavior selectedListenerFailureBehavior() {
                    return ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT;
                }
            };

    static {
        Integer dhLen = null;

        try {
            String dhKeySize = AccessController.doPrivileged(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    return SystemPropertyUtil.get("jdk.tls.ephemeralDHKeySize");
                }
            });
            if (dhKeySize != null) {
                try {
                    dhLen = Integer.valueOf(dhKeySize);
                } catch (NumberFormatException e) {
                    logger.debug("ReferenceCountedOpenSslContext supports -Djdk.tls.ephemeralDHKeySize={int}, but got: "
                            + dhKeySize);
                }
            }
        } catch (Throwable ignore) {
            // ignore
        }
        DH_KEY_LENGTH = dhLen;
    }

    ReferenceCountedOpenSslContext(Iterable<String> ciphers, CipherSuiteFilter cipherFilter,
                                   ApplicationProtocolConfig apnCfg, long sessionCacheSize, long sessionTimeout,
                                   int mode, Certificate[] keyCertChain, ClientAuth clientAuth, String[] protocols,
                                   boolean startTls, boolean enableOcsp, boolean leakDetection) throws SSLException {
        this(ciphers, cipherFilter, toNegotiator(apnCfg), sessionCacheSize, sessionTimeout, mode, keyCertChain,
                clientAuth, protocols, startTls, enableOcsp, leakDetection);
    }

    ReferenceCountedOpenSslContext(Iterable<String> ciphers, CipherSuiteFilter cipherFilter,
                                   OpenSslApplicationProtocolNegotiator apn, long sessionCacheSize,
                                   long sessionTimeout, int mode, Certificate[] keyCertChain,
                                   ClientAuth clientAuth, String[] protocols, boolean startTls, boolean enableOcsp,
                                   boolean leakDetection) throws SSLException {
        super(startTls);

        OpenSsl.ensureAvailability();

        if (enableOcsp && !OpenSsl.isOcspSupported()) {
            throw new IllegalStateException("OCSP is not supported.");
        }

        if (mode != SSL.SSL_MODE_SERVER && mode != SSL.SSL_MODE_CLIENT) {
            throw new IllegalArgumentException("mode most be either SSL.SSL_MODE_SERVER or SSL.SSL_MODE_CLIENT");
        }
        leak = leakDetection ? leakDetector.track(this) : null;
        this.mode = mode;
        this.clientAuth = isServer() ? checkNotNull(clientAuth, "clientAuth") : ClientAuth.NONE;
        this.protocols = protocols;
        this.enableOcsp = enableOcsp;

        if (mode == SSL.SSL_MODE_SERVER) {
            rejectRemoteInitiatedRenegotiation =
                    JDK_REJECT_CLIENT_INITIATED_RENEGOTIATION;
        }
        this.keyCertChain = keyCertChain == null ? null : keyCertChain.clone();

        unmodifiableCiphers = Arrays.asList(checkNotNull(cipherFilter, "cipherFilter").filterCipherSuites(
                ciphers, DEFAULT_CIPHERS, availableJavaCipherSuites()));

        this.apn = checkNotNull(apn, "apn");

        // Create a new SSL_CTX and configure it.
        boolean success = false;
        try {
            try {
                ctx = SSLContext.make(SSL.SSL_PROTOCOL_ALL, mode);
            } catch (Exception e) {
                throw new SSLException("failed to create an SSL_CTX", e);
            }

            SSLContext.setOptions(ctx, SSLContext.getOptions(ctx) |
                    SSL.SSL_OP_NO_SSLv2 |
                    SSL.SSL_OP_NO_SSLv3 |
                    SSL.SSL_OP_CIPHER_SERVER_PREFERENCE |

                    // We do not support compression at the moment so we should explicitly disable it.
                    SSL.SSL_OP_NO_COMPRESSION |

                    // Disable ticket support by default to be more inline with SSLEngineImpl of the JDK.
                    // This also let SSLSession.getId() work the same way for the JDK implementation and the
                    // OpenSSLEngine. If tickets are supported SSLSession.getId() will only return an ID on the
                    // server-side if it could make use of tickets.
                    SSL.SSL_OP_NO_TICKET);

            // We need to enable SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER as the memory address may change between
            // calling OpenSSLEngine.wrap(...).
            // See https://github.com/netty/netty-tcnative/issues/100
            SSLContext.setMode(ctx, SSLContext.getMode(ctx) | SSL.SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

            if (DH_KEY_LENGTH != null) {
                SSLContext.setTmpDHLength(ctx, DH_KEY_LENGTH);
            }

                /* List the ciphers that are permitted to negotiate. */
            try {
                SSLContext.setCipherSuite(ctx, CipherSuiteConverter.toOpenSsl(unmodifiableCiphers));
            } catch (SSLException e) {
                throw e;
            } catch (Exception e) {
                throw new SSLException("failed to set cipher suite: " + unmodifiableCiphers, e);
            }

            List<String> nextProtoList = apn.protocols();
                /* Set next protocols for next protocol negotiation extension, if specified */
            if (!nextProtoList.isEmpty()) {
                String[] appProtocols = nextProtoList.toArray(new String[nextProtoList.size()]);
                int selectorBehavior = opensslSelectorFailureBehavior(apn.selectorFailureBehavior());

                switch (apn.protocol()) {
                    case NPN:
                        SSLContext.setNpnProtos(ctx, appProtocols, selectorBehavior);
                        break;
                    case ALPN:
                        SSLContext.setAlpnProtos(ctx, appProtocols, selectorBehavior);
                        break;
                    case NPN_AND_ALPN:
                        SSLContext.setNpnProtos(ctx, appProtocols, selectorBehavior);
                        SSLContext.setAlpnProtos(ctx, appProtocols, selectorBehavior);
                        break;
                    default:
                        throw new Error();
                }
            }

            /* Set session cache size, if specified */
            if (sessionCacheSize > 0) {
                this.sessionCacheSize = sessionCacheSize;
                SSLContext.setSessionCacheSize(ctx, sessionCacheSize);
            } else {
                // Get the default session cache size using SSLContext.setSessionCacheSize()
                this.sessionCacheSize = sessionCacheSize = SSLContext.setSessionCacheSize(ctx, 20480);
                // Revert the session cache size to the default value.
                SSLContext.setSessionCacheSize(ctx, sessionCacheSize);
            }

            /* Set session timeout, if specified */
            if (sessionTimeout > 0) {
                this.sessionTimeout = sessionTimeout;
                SSLContext.setSessionCacheTimeout(ctx, sessionTimeout);
            } else {
                // Get the default session timeout using SSLContext.setSessionCacheTimeout()
                this.sessionTimeout = sessionTimeout = SSLContext.setSessionCacheTimeout(ctx, 300);
                // Revert the session timeout to the default value.
                SSLContext.setSessionCacheTimeout(ctx, sessionTimeout);
            }

            if (enableOcsp) {
                SSLContext.enableOcsp(ctx, isClient());
            }
            success = true;
        } finally {
            if (!success) {
                release();
            }
        }
    }

    private static int opensslSelectorFailureBehavior(ApplicationProtocolConfig.SelectorFailureBehavior behavior) {
        switch (behavior) {
            case NO_ADVERTISE:
                return SSL.SSL_SELECTOR_FAILURE_NO_ADVERTISE;
            case CHOOSE_MY_LAST_PROTOCOL:
                return SSL.SSL_SELECTOR_FAILURE_CHOOSE_MY_LAST_PROTOCOL;
            default:
                throw new Error();
        }
    }

    @Override
    public final List<String> cipherSuites() {
        return unmodifiableCiphers;
    }

    @Override
    public final long sessionCacheSize() {
        return sessionCacheSize;
    }

    @Override
    public final long sessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public ApplicationProtocolNegotiator applicationProtocolNegotiator() {
        return apn;
    }

    @Override
    public final boolean isClient() {
        return mode == SSL.SSL_MODE_CLIENT;
    }

    @Override
    public final SSLEngine newEngine(ByteBufAllocator alloc, String peerHost, int peerPort) {
        return newEngine0(alloc, peerHost, peerPort, true);
    }

    @Override
    protected final SslHandler newHandler(ByteBufAllocator alloc, boolean startTls) {
        return new SslHandler(newEngine0(alloc, null, -1, false), startTls, false);
    }

    @Override
    protected final SslHandler newHandler(ByteBufAllocator alloc, String peerHost, int peerPort, boolean startTls) {
        return new SslHandler(newEngine0(alloc, peerHost, peerPort, false), startTls, false);
    }

    SSLEngine newEngine0(ByteBufAllocator alloc, String peerHost, int peerPort, boolean jdkCompatibilityMode) {
        return new ReferenceCountedOpenSslEngine(this, alloc, peerHost, peerPort, jdkCompatibilityMode, true);
    }

    abstract OpenSslKeyMaterialManager keyMaterialManager();

    /**
     * Returns a new server-side {@link SSLEngine} with the current configuration.
     */
    @Override
    public final SSLEngine newEngine(ByteBufAllocator alloc) {
        return newEngine(alloc, null, -1);
    }

    /**
     * Returns the pointer to the {@code SSL_CTX} object for this {@link ReferenceCountedOpenSslContext}.
     * Be aware that it is freed as soon as the {@link #finalize()}  method is called.
     * At this point {@code 0} will be returned.
     *
     * @deprecated this method is considered unsafe as the returned pointer may be released later. Dont use it!
     */
    @Deprecated
    public final long context() {
        Lock readerLock = ctxLock.readLock();
        readerLock.lock();
        try {
            return ctx;
        } finally {
            readerLock.unlock();
        }
    }

    /**
     * Returns the stats of this context.
     *
     * @deprecated use {@link #sessionContext#stats()}
     */
    @Deprecated
    public final OpenSslSessionStats stats() {
        return sessionContext().stats();
    }

    /**
     * Specify if remote initiated renegotiation is supported or not. If not supported and the remote side tries
     * to initiate a renegotiation a {@link SSLHandshakeException} will be thrown during decoding.
     */
    public void setRejectRemoteInitiatedRenegotiation(boolean rejectRemoteInitiatedRenegotiation) {
        this.rejectRemoteInitiatedRenegotiation = rejectRemoteInitiatedRenegotiation;
    }

    /**
     * Returns if remote initiated renegotiation is supported or not.
     */
    public boolean getRejectRemoteInitiatedRenegotiation() {
        return rejectRemoteInitiatedRenegotiation;
    }

    /**
     * Set the size of the buffer used by the BIO for non-application based writes
     * (e.g. handshake, renegotiation, etc...).
     */
    public void setBioNonApplicationBufferSize(int bioNonApplicationBufferSize) {
        this.bioNonApplicationBufferSize =
                checkPositiveOrZero(bioNonApplicationBufferSize, "bioNonApplicationBufferSize");
    }

    /**
     * Returns the size of the buffer used by the BIO for non-application based writes
     */
    public int getBioNonApplicationBufferSize() {
        return bioNonApplicationBufferSize;
    }

    /**
     * Sets the SSL session ticket keys of this context.
     *
     * @deprecated use {@link OpenSslSessionContext#setTicketKeys(byte[])}
     */
    @Deprecated
    public final void setTicketKeys(byte[] keys) {
        sessionContext().setTicketKeys(keys);
    }

    @Override
    public abstract OpenSslSessionContext sessionContext();

    /**
     * Returns the pointer to the {@code SSL_CTX} object for this {@link ReferenceCountedOpenSslContext}.
     * Be aware that it is freed as soon as the {@link #release()} method is called.
     * At this point {@code 0} will be returned.
     *
     * @deprecated this method is considered unsafe as the returned pointer may be released later. Dont use it!
     */
    @Deprecated
    public final long sslCtxPointer() {
        Lock readerLock = ctxLock.readLock();
        readerLock.lock();
        try {
            return ctx;
        } finally {
            readerLock.unlock();
        }
    }

    // IMPORTANT: This method must only be called from either the constructor or the finalizer as a user MUST never
    //            get access to an OpenSslSessionContext after this method was called to prevent the user from
    //            producing a segfault.
    private void destroy() {
        Lock writerLock = ctxLock.writeLock();
        writerLock.lock();
        try {
            if (ctx != 0) {
                if (enableOcsp) {
                    SSLContext.disableOcsp(ctx);
                }

                SSLContext.free(ctx);
                ctx = 0;
            }
        } finally {
            writerLock.unlock();
        }
    }

    protected static X509Certificate[] certificates(byte[][] chain) {
        X509Certificate[] peerCerts = new X509Certificate[chain.length];
        for (int i = 0; i < peerCerts.length; i++) {
            peerCerts[i] = new OpenSslX509Certificate(chain[i]);
        }
        return peerCerts;
    }

    protected static X509TrustManager chooseTrustManager(TrustManager[] managers) {
        for (TrustManager m : managers) {
            if (m instanceof X509TrustManager) {
                return (X509TrustManager) m;
            }
        }
        throw new IllegalStateException("no X509TrustManager found");
    }

    protected static X509KeyManager chooseX509KeyManager(KeyManager[] kms) {
        for (KeyManager km : kms) {
            if (km instanceof X509KeyManager) {
                return (X509KeyManager) km;
            }
        }
        throw new IllegalStateException("no X509KeyManager found");
    }

    /**
     * Translate a {@link ApplicationProtocolConfig} object to a
     * {@link OpenSslApplicationProtocolNegotiator} object.
     *
     * @param config The configuration which defines the translation
     * @return The results of the translation
     */
    @SuppressWarnings("deprecation")
    static OpenSslApplicationProtocolNegotiator toNegotiator(ApplicationProtocolConfig config) {
        if (config == null) {
            return NONE_PROTOCOL_NEGOTIATOR;
        }

        switch (config.protocol()) {
            case NONE:
                return NONE_PROTOCOL_NEGOTIATOR;
            case ALPN:
            case NPN:
            case NPN_AND_ALPN:
                switch (config.selectedListenerFailureBehavior()) {
                    case CHOOSE_MY_LAST_PROTOCOL:
                    case ACCEPT:
                        switch (config.selectorFailureBehavior()) {
                            case CHOOSE_MY_LAST_PROTOCOL:
                            case NO_ADVERTISE:
                                return new OpenSslDefaultApplicationProtocolNegotiator(
                                        config);
                            default:
                                throw new UnsupportedOperationException(
                                        new StringBuilder("OpenSSL provider does not support ")
                                                .append(config.selectorFailureBehavior())
                                                .append(" behavior").toString());
                        }
                    default:
                        throw new UnsupportedOperationException(
                                new StringBuilder("OpenSSL provider does not support ")
                                        .append(config.selectedListenerFailureBehavior())
                                        .append(" behavior").toString());
                }
            default:
                throw new Error();
        }
    }

    static boolean useExtendedTrustManager(X509TrustManager trustManager) {
        return PlatformDependent.javaVersion() >= 7 && trustManager instanceof X509ExtendedTrustManager;
    }

    static boolean useExtendedKeyManager(X509KeyManager keyManager) {
        return PlatformDependent.javaVersion() >= 7 && keyManager instanceof X509ExtendedKeyManager;
    }

    @Override
    public final int refCnt() {
        return refCnt.refCnt();
    }

    @Override
    public final ReferenceCounted retain() {
        refCnt.retain();
        return this;
    }

    @Override
    public final ReferenceCounted retain(int increment) {
        refCnt.retain(increment);
        return this;
    }

    @Override
    public final ReferenceCounted touch() {
        refCnt.touch();
        return this;
    }

    @Override
    public final ReferenceCounted touch(Object hint) {
        refCnt.touch(hint);
        return this;
    }

    @Override
    public final boolean release() {
        return refCnt.release();
    }

    @Override
    public final boolean release(int decrement) {
        return refCnt.release(decrement);
    }

    abstract static class AbstractCertificateVerifier extends CertificateVerifier {
        private final OpenSslEngineMap engineMap;

        AbstractCertificateVerifier(OpenSslEngineMap engineMap) {
            this.engineMap = engineMap;
        }

        @Override
        public final int verify(long ssl, byte[][] chain, String auth) {
            X509Certificate[] peerCerts = certificates(chain);
            final ReferenceCountedOpenSslEngine engine = engineMap.get(ssl);
            try {
                verify(engine, peerCerts, auth);
                return CertificateVerifier.X509_V_OK;
            } catch (Throwable cause) {
                logger.debug("verification of certificate failed", cause);
                SSLHandshakeException e = new SSLHandshakeException("General OpenSslEngine problem");
                e.initCause(cause);
                engine.handshakeException = e;

                // Try to extract the correct error code that should be used.
                if (cause instanceof OpenSslCertificateException) {
                    // This will never return a negative error code as its validated when constructing the
                    // OpenSslCertificateException.
                    return ((OpenSslCertificateException) cause).errorCode();
                }
                if (cause instanceof CertificateExpiredException) {
                    return CertificateVerifier.X509_V_ERR_CERT_HAS_EXPIRED;
                }
                if (cause instanceof CertificateNotYetValidException) {
                    return CertificateVerifier.X509_V_ERR_CERT_NOT_YET_VALID;
                }
                if (PlatformDependent.javaVersion() >= 7) {
                    if (cause instanceof CertificateRevokedException) {
                        return CertificateVerifier.X509_V_ERR_CERT_REVOKED;
                    }

                    // The X509TrustManagerImpl uses a Validator which wraps a CertPathValidatorException into
                    // an CertificateException. So we need to handle the wrapped CertPathValidatorException to be
                    // able to send the correct alert.
                    Throwable wrapped = cause.getCause();
                    while (wrapped != null) {
                        if (wrapped instanceof CertPathValidatorException) {
                            CertPathValidatorException ex = (CertPathValidatorException) wrapped;
                            CertPathValidatorException.Reason reason = ex.getReason();
                            if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                                return CertificateVerifier.X509_V_ERR_CERT_HAS_EXPIRED;
                            }
                            if (reason == CertPathValidatorException.BasicReason.NOT_YET_VALID) {
                                return CertificateVerifier.X509_V_ERR_CERT_NOT_YET_VALID;
                            }
                            if (reason == CertPathValidatorException.BasicReason.REVOKED) {
                                return CertificateVerifier.X509_V_ERR_CERT_REVOKED;
                            }
                        }
                        wrapped = wrapped.getCause();
                    }
                }

                // Could not detect a specific error code to use, so fallback to a default code.
                return CertificateVerifier.X509_V_ERR_UNSPECIFIED;
            }
        }

        abstract void verify(ReferenceCountedOpenSslEngine engine, X509Certificate[] peerCerts,
                             String auth) throws Exception;
    }

    private static final class DefaultOpenSslEngineMap implements OpenSslEngineMap {
        private final Map<Long, ReferenceCountedOpenSslEngine> engines = PlatformDependent.newConcurrentHashMap();

        @Override
        public ReferenceCountedOpenSslEngine remove(long ssl) {
            return engines.remove(ssl);
        }

        @Override
        public void add(ReferenceCountedOpenSslEngine engine) {
            engines.put(engine.sslPointer(), engine);
        }

        @Override
        public ReferenceCountedOpenSslEngine get(long ssl) {
            return engines.get(ssl);
        }
    }

    static void setKeyMaterial(long ctx, X509Certificate[] keyCertChain, PrivateKey key, String keyPassword)
            throws SSLException {
         /* Load the certificate file and private key. */
        long keyBio = 0;
        long keyCertChainBio = 0;
        long keyCertChainBio2 = 0;
        PemEncoded encoded = null;
        try {
            // Only encode one time
            encoded = PemX509Certificate.toPEM(ByteBufAllocator.DEFAULT, true, keyCertChain);
            keyCertChainBio = toBIO(ByteBufAllocator.DEFAULT, encoded.retain());
            keyCertChainBio2 = toBIO(ByteBufAllocator.DEFAULT, encoded.retain());

            if (key != null) {
                keyBio = toBIO(key);
            }

            SSLContext.setCertificateBio(
                    ctx, keyCertChainBio, keyBio,
                    keyPassword == null ? StringUtil.EMPTY_STRING : keyPassword);
            // We may have more then one cert in the chain so add all of them now.
            SSLContext.setCertificateChainBio(ctx, keyCertChainBio2, true);
        } catch (SSLException e) {
            throw e;
        } catch (Exception e) {
            throw new SSLException("failed to set certificate and key", e);
        } finally {
            freeBio(keyBio);
            freeBio(keyCertChainBio);
            freeBio(keyCertChainBio2);
            if (encoded != null) {
                encoded.release();
            }
        }
    }

    static void freeBio(long bio) {
        if (bio != 0) {
            SSL.freeBIO(bio);
        }
    }

    /**
     * Return the pointer to a <a href="https://www.openssl.org/docs/crypto/BIO_get_mem_ptr.html">in-memory BIO</a>
     * or {@code 0} if the {@code key} is {@code null}. The BIO contains the content of the {@code key}.
     */
    static long toBIO(PrivateKey key) throws Exception {
        if (key == null) {
            return 0;
        }

        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        PemEncoded pem = PemPrivateKey.toPEM(allocator, true, key);
        try {
            return toBIO(allocator, pem.retain());
        } finally {
            pem.release();
        }
    }

    /**
     * Return the pointer to a <a href="https://www.openssl.org/docs/crypto/BIO_get_mem_ptr.html">in-memory BIO</a>
     * or {@code 0} if the {@code certChain} is {@code null}. The BIO contains the content of the {@code certChain}.
     */
    static long toBIO(X509Certificate... certChain) throws Exception {
        if (certChain == null) {
            return 0;
        }

        if (certChain.length == 0) {
            throw new IllegalArgumentException("certChain can't be empty");
        }

        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        PemEncoded pem = PemX509Certificate.toPEM(allocator, true, certChain);
        try {
            return toBIO(allocator, pem.retain());
        } finally {
            pem.release();
        }
    }

    static long toBIO(ByteBufAllocator allocator, PemEncoded pem) throws Exception {
        try {
            // We can turn direct buffers straight into BIOs. No need to
            // make a yet another copy.
            ByteBuf content = pem.content();

            if (content.isDirect()) {
                return newBIO(content.retainedSlice());
            }

            ByteBuf buffer = allocator.directBuffer(content.readableBytes());
            try {
                buffer.writeBytes(content, content.readerIndex(), content.readableBytes());
                return newBIO(buffer.retainedSlice());
            } finally {
                try {
                    // If the contents of the ByteBuf is sensitive (e.g. a PrivateKey) we
                    // need to zero out the bytes of the copy before we're releasing it.
                    if (pem.isSensitive()) {
                        SslUtils.zeroout(buffer);
                    }
                } finally {
                    buffer.release();
                }
            }
        } finally {
            pem.release();
        }
    }

    private static long newBIO(ByteBuf buffer) throws Exception {
        try {
            long bio = SSL.newMemBIO();
            int readable = buffer.readableBytes();
            if (SSL.bioWrite(bio, OpenSsl.memoryAddress(buffer) + buffer.readerIndex(), readable) != readable) {
                SSL.freeBIO(bio);
                throw new IllegalStateException("Could not write data to memory BIO");
            }
            return bio;
        } finally {
            buffer.release();
        }
    }
}
