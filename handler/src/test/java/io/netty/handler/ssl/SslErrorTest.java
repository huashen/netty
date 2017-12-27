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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.ssl.util.SimpleTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.EmptyArrays;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.security.KeyStore;
import java.security.cert.CRLReason;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CertificateRevokedException;
import java.security.cert.Extension;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;


@RunWith(Parameterized.class)
public class SslErrorTest {

    @Parameterized.Parameters(name = "{index}: serverProvider = {0}, clientProvider = {1}, exception = {2}")
    public static Collection<Object[]> data() {
        List<SslProvider> serverProviders = new ArrayList<SslProvider>(2);
        List<SslProvider> clientProviders = new ArrayList<SslProvider>(3);

        if (OpenSsl.isAvailable()) {
            serverProviders.add(SslProvider.OPENSSL);
            serverProviders.add(SslProvider.OPENSSL_REFCNT);
            clientProviders.add(SslProvider.OPENSSL);
            clientProviders.add(SslProvider.OPENSSL_REFCNT);
        }
        // We not test with SslProvider.JDK on the server side as the JDK implementation currently just send the same
        // alert all the time, sigh.....
        clientProviders.add(SslProvider.JDK);

        List<CertificateException> exceptions = new ArrayList<CertificateException>(6);
        exceptions.add(new CertificateExpiredException());
        exceptions.add(new CertificateNotYetValidException());
        exceptions.add(new CertificateRevokedException(
                new Date(), CRLReason.AA_COMPROMISE, new X500Principal(""),
                Collections.<String, Extension>emptyMap()));

        // Also use wrapped exceptions as this is what the JDK implementation of X509TrustManagerFactory is doing.
        exceptions.add(newCertificateException(CertPathValidatorException.BasicReason.EXPIRED));
        exceptions.add(newCertificateException(CertPathValidatorException.BasicReason.NOT_YET_VALID));
        exceptions.add(newCertificateException(CertPathValidatorException.BasicReason.REVOKED));

        List<Object[]> params = new ArrayList<Object[]>();
        for (SslProvider serverProvider: serverProviders) {
            for (SslProvider clientProvider: clientProviders) {
                for (CertificateException exception: exceptions) {
                    params.add(new Object[] { serverProvider, clientProvider, exception});
                }
            }
        }
        return params;
    }

    private static CertificateException newCertificateException(CertPathValidatorException.Reason reason) {
        return new TestCertificateException(
                new CertPathValidatorException("x", null, null, -1, reason));
    }

    private final SslProvider serverProvider;
    private final SslProvider clientProvider;
    private final CertificateException exception;

    public SslErrorTest(SslProvider serverProvider, SslProvider clientProvider, CertificateException exception) {
        this.serverProvider = serverProvider;
        this.clientProvider = clientProvider;
        this.exception = exception;
    }

    @Test(timeout = 30000)
    public void testCorrectAlert() throws Exception {
        // As this only works correctly at the moment when OpenSslEngine is used on the server-side there is
        // no need to run it if there is no openssl is available at all.
        Assume.assumeTrue(OpenSsl.isAvailable());

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        final SslContext sslServerCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .sslProvider(serverProvider)
                .trustManager(new SimpleTrustManagerFactory() {
            @Override
            protected void engineInit(KeyStore keyStore) { }
            @Override
            protected void engineInit(ManagerFactoryParameters managerFactoryParameters) { }

            @Override
            protected TrustManager[] engineGetTrustManagers() {
                return new TrustManager[] { new X509TrustManager() {

                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException {
                        throw exception;
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                            throws CertificateException {
                        // NOOP
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return EmptyArrays.EMPTY_X509_CERTIFICATES;
                    }
                } };
            }
        }).clientAuth(ClientAuth.REQUIRE).build();

        final SslContext sslClientCtx = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .keyManager(new File(getClass().getResource("test.crt").getFile()),
                        new File(getClass().getResource("test_unencrypted.pem").getFile()))
                .sslProvider(clientProvider).build();

        Channel serverChannel = null;
        Channel clientChannel = null;
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            serverChannel = new ServerBootstrap().group(group)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline().addLast(sslServerCtx.newHandler(ch.alloc()));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    ctx.close();
                                }
                            });
                        }
                    }).bind(0).sync().channel();

            final Promise<Void> promise = group.next().newPromise();

            clientChannel = new Bootstrap().group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline().addLast(sslClientCtx.newHandler(ch.alloc()));
                            ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    // Unwrap as its wrapped by a DecoderException
                                    Throwable unwrappedCause = cause.getCause();
                                    if (unwrappedCause instanceof SSLException) {
                                        if (exception instanceof TestCertificateException) {
                                            CertPathValidatorException.Reason reason =
                                                    ((CertPathValidatorException) exception.getCause()).getReason();
                                            if (reason == CertPathValidatorException.BasicReason.EXPIRED) {
                                                verifyException(unwrappedCause, "expired", promise);
                                            } else if (reason == CertPathValidatorException.BasicReason.NOT_YET_VALID) {
                                                verifyException(unwrappedCause, "bad", promise);
                                            } else if (reason == CertPathValidatorException.BasicReason.REVOKED) {
                                                verifyException(unwrappedCause, "revoked", promise);
                                            }
                                        } else if (exception instanceof CertificateExpiredException) {
                                            verifyException(unwrappedCause, "expired", promise);
                                        } else if (exception instanceof CertificateNotYetValidException) {
                                            verifyException(unwrappedCause, "bad", promise);
                                        } else if (exception instanceof CertificateRevokedException) {
                                            verifyException(unwrappedCause, "revoked", promise);
                                        }
                                    }
                                }
                            });
                        }
                    }).connect(serverChannel.localAddress()).syncUninterruptibly().channel();
            // Block until we received the correct exception
            promise.syncUninterruptibly();
        } finally {
            if (clientChannel != null) {
                clientChannel.close().syncUninterruptibly();
            }
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            group.shutdownGracefully();

            ReferenceCountUtil.release(sslServerCtx);
            ReferenceCountUtil.release(sslClientCtx);
        }
    }

    // Its a bit hacky to verify against the message that is part of the exception but there is no other way
    // at the moment as there are no different exceptions for the different alerts.
    private static void verifyException(Throwable cause, String messagePart, Promise<Void> promise) {
        String message = cause.getMessage();
        if (message.toLowerCase(Locale.UK).contains(messagePart.toLowerCase(Locale.UK))) {
            promise.setSuccess(null);
        } else {
            promise.setFailure(new AssertionError("message not contains '" + messagePart + "': " + message));
        }
    }

    private static final class TestCertificateException extends CertificateException {

        public TestCertificateException(Throwable cause) {
            super(cause);
        }
    }
}
