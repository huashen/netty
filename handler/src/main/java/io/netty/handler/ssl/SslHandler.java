/*
 * Copyright 2012 The Netty Project
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractCoalescingBufferQueue;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPromiseNotifier;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import static io.netty.buffer.ByteBufUtil.ensureWritableSuccess;
import static io.netty.handler.ssl.SslUtils.getEncryptedPacketLength;

/**
 * Adds <a href="http://en.wikipedia.org/wiki/Transport_Layer_Security">SSL
 * &middot; TLS</a> and StartTLS support to a {@link Channel}.  Please refer
 * to the <strong>"SecureChat"</strong> example in the distribution or the web
 * site for the detailed usage.
 *
 * <h3>Beginning the handshake</h3>
 * <p>
 * Beside using the handshake {@link ChannelFuture} to get notified about the completion of the handshake it's
 * also possible to detect it by implement the
 * {@link ChannelInboundHandler#userEventTriggered(ChannelHandlerContext, Object)}
 * method and check for a {@link SslHandshakeCompletionEvent}.
 *
 * <h3>Handshake</h3>
 * <p>
 * The handshake will be automatically issued for you once the {@link Channel} is active and
 * {@link SSLEngine#getUseClientMode()} returns {@code true}.
 * So no need to bother with it by your self.
 *
 * <h3>Closing the session</h3>
 * <p>
 * To close the SSL session, the {@link #close()} method should be
 * called to send the {@code close_notify} message to the remote peer.  One
 * exception is when you close the {@link Channel} - {@link SslHandler}
 * intercepts the close request and send the {@code close_notify} message
 * before the channel closure automatically.  Once the SSL session is closed,
 * it is not reusable, and consequently you should create a new
 * {@link SslHandler} with a new {@link SSLEngine} as explained in the
 * following section.
 *
 * <h3>Restarting the session</h3>
 * <p>
 * To restart the SSL session, you must remove the existing closed
 * {@link SslHandler} from the {@link ChannelPipeline}, insert a new
 * {@link SslHandler} with a new {@link SSLEngine} into the pipeline,
 * and start the handshake process as described in the first section.
 *
 * <h3>Implementing StartTLS</h3>
 * <p>
 * <a href="http://en.wikipedia.org/wiki/STARTTLS">StartTLS</a> is the
 * communication pattern that secures the wire in the middle of the plaintext
 * connection.  Please note that it is different from SSL &middot; TLS, that
 * secures the wire from the beginning of the connection.  Typically, StartTLS
 * is composed of three steps:
 * <ol>
 * <li>Client sends a StartTLS request to server.</li>
 * <li>Server sends a StartTLS response to client.</li>
 * <li>Client begins SSL handshake.</li>
 * </ol>
 * If you implement a server, you need to:
 * <ol>
 * <li>create a new {@link SslHandler} instance with {@code startTls} flag set
 *     to {@code true},</li>
 * <li>insert the {@link SslHandler} to the {@link ChannelPipeline}, and</li>
 * <li>write a StartTLS response.</li>
 * </ol>
 * Please note that you must insert {@link SslHandler} <em>before</em> sending
 * the StartTLS response.  Otherwise the client can send begin SSL handshake
 * before {@link SslHandler} is inserted to the {@link ChannelPipeline}, causing
 * data corruption.
 * <p>
 * The client-side implementation is much simpler.
 * <ol>
 * <li>Write a StartTLS request,</li>
 * <li>wait for the StartTLS response,</li>
 * <li>create a new {@link SslHandler} instance with {@code startTls} flag set
 *     to {@code false},</li>
 * <li>insert the {@link SslHandler} to the {@link ChannelPipeline}, and</li>
 * <li>Initiate SSL handshake.</li>
 * </ol>
 *
 * <h3>Known issues</h3>
 * <p>
 * Because of a known issue with the current implementation of the SslEngine that comes
 * with Java it may be possible that you see blocked IO-Threads while a full GC is done.
 * <p>
 * So if you are affected you can workaround this problem by adjust the cache settings
 * like shown below:
 *
 * <pre>
 *     SslContext context = ...;
 *     context.getServerSessionContext().setSessionCacheSize(someSaneSize);
 *     context.getServerSessionContext().setSessionTime(someSameTimeout);
 * </pre>
 * <p>
 * What values to use here depends on the nature of your application and should be set
 * based on monitoring and debugging of it.
 * For more details see
 * <a href="https://github.com/netty/netty/issues/832">#832</a> in our issue tracker.
 */
public class SslHandler extends ByteToMessageDecoder implements ChannelOutboundHandler {

    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(SslHandler.class);

    private static final Pattern IGNORABLE_CLASS_IN_STACK = Pattern.compile(
            "^.*(?:Socket|Datagram|Sctp|Udt)Channel.*$");
    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$", Pattern.CASE_INSENSITIVE);

    /**
     * Used in {@link #unwrapNonAppData(ChannelHandlerContext)} as input for
     * {@link #unwrap(ChannelHandlerContext, ByteBuf, int,  int)}.  Using this static instance reduce object
     * creation as {@link Unpooled#EMPTY_BUFFER#nioBuffer()} creates a new {@link ByteBuffer} everytime.
     */
    private static final SSLException SSLENGINE_CLOSED = ThrowableUtil.unknownStackTrace(
            new SSLException("SSLEngine closed already"), SslHandler.class, "wrap(...)");
    private static final SSLException HANDSHAKE_TIMED_OUT = ThrowableUtil.unknownStackTrace(
            new SSLException("handshake timed out"), SslHandler.class, "handshake(...)");
    private static final ClosedChannelException CHANNEL_CLOSED = ThrowableUtil.unknownStackTrace(
            new ClosedChannelException(), SslHandler.class, "channelInactive(...)");

    /**
     * <a href="https://tools.ietf.org/html/rfc5246#section-6.2">2^14</a> which is the maximum sized plaintext chunk
     * allowed by the TLS RFC.
     */
    private static final int MAX_PLAINTEXT_LENGTH = 16 * 1024;

    private enum SslEngineType {
        TCNATIVE(true, COMPOSITE_CUMULATOR) {
            @Override
            SSLEngineResult unwrap(SslHandler handler, ByteBuf in, int readerIndex, int len, ByteBuf out)
                    throws SSLException {
                int nioBufferCount = in.nioBufferCount();
                int writerIndex = out.writerIndex();
                final SSLEngineResult result;
                if (nioBufferCount > 1) {
                    /*
                     * If {@link OpenSslEngine} is in use,
                     * we can use a special {@link OpenSslEngine#unwrap(ByteBuffer[], ByteBuffer[])} method
                     * that accepts multiple {@link ByteBuffer}s without additional memory copies.
                     */
                    ReferenceCountedOpenSslEngine opensslEngine = (ReferenceCountedOpenSslEngine) handler.engine;
                    try {
                        handler.singleBuffer[0] = toByteBuffer(out, writerIndex,
                            out.writableBytes());
                        result = opensslEngine.unwrap(in.nioBuffers(readerIndex, len), handler.singleBuffer);
                    } finally {
                        handler.singleBuffer[0] = null;
                    }
                } else {
                    result = handler.engine.unwrap(toByteBuffer(in, readerIndex, len),
                        toByteBuffer(out, writerIndex, out.writableBytes()));
                }
                out.writerIndex(writerIndex + result.bytesProduced());
                return result;
            }

            @Override
            int getPacketBufferSize(SslHandler handler) {
                return ((ReferenceCountedOpenSslEngine) handler.engine).maxEncryptedPacketLength0();
            }

            @Override
            int calculateWrapBufferCapacity(SslHandler handler, int pendingBytes, int numComponents) {
                return ((ReferenceCountedOpenSslEngine) handler.engine).calculateMaxLengthForWrap(pendingBytes,
                                                                                                  numComponents);
            }

            @Override
            int calculatePendingData(SslHandler handler, int guess) {
                int sslPending = ((ReferenceCountedOpenSslEngine) handler.engine).sslPending();
                return sslPending > 0 ? sslPending : guess;
            }
        },
        CONSCRYPT(true, COMPOSITE_CUMULATOR) {
            @Override
            SSLEngineResult unwrap(SslHandler handler, ByteBuf in, int readerIndex, int len, ByteBuf out)
                    throws SSLException {
                int nioBufferCount = in.nioBufferCount();
                int writerIndex = out.writerIndex();
                final SSLEngineResult result;
                if (nioBufferCount > 1) {
                    /*
                     * Use a special unwrap method without additional memory copies.
                     */
                    try {
                        handler.singleBuffer[0] = toByteBuffer(out, writerIndex, out.writableBytes());
                        result = ((ConscryptAlpnSslEngine) handler.engine).unwrap(
                                in.nioBuffers(readerIndex, len),
                                handler.singleBuffer);
                    } finally {
                        handler.singleBuffer[0] = null;
                    }
                } else {
                    result = handler.engine.unwrap(toByteBuffer(in, readerIndex, len),
                            toByteBuffer(out, writerIndex, out.writableBytes()));
                }
                out.writerIndex(writerIndex + result.bytesProduced());
                return result;
            }

            @Override
            int calculateWrapBufferCapacity(SslHandler handler, int pendingBytes, int numComponents) {
                return ((ConscryptAlpnSslEngine) handler.engine).calculateOutNetBufSize(pendingBytes, numComponents);
            }

            @Override
            int calculatePendingData(SslHandler handler, int guess) {
                return guess;
            }
        },
        JDK(false, MERGE_CUMULATOR) {
            @Override
            SSLEngineResult unwrap(SslHandler handler, ByteBuf in, int readerIndex, int len, ByteBuf out)
                    throws SSLException {
                int writerIndex = out.writerIndex();
                final SSLEngineResult result = handler.engine.unwrap(toByteBuffer(in, readerIndex, len),
                    toByteBuffer(out, writerIndex, out.writableBytes()));
                out.writerIndex(writerIndex + result.bytesProduced());
                return result;
            }

            @Override
            int calculateWrapBufferCapacity(SslHandler handler, int pendingBytes, int numComponents) {
                return handler.engine.getSession().getPacketBufferSize();
            }

            @Override
            int calculatePendingData(SslHandler handler, int guess) {
                return guess;
            }
        };

        static SslEngineType forEngine(SSLEngine engine) {
            return engine instanceof ReferenceCountedOpenSslEngine ? TCNATIVE :
                   engine instanceof ConscryptAlpnSslEngine ? CONSCRYPT : JDK;
        }

        SslEngineType(boolean wantsDirectBuffer, Cumulator cumulator) {
            this.wantsDirectBuffer = wantsDirectBuffer;
            this.cumulator = cumulator;
        }

        int getPacketBufferSize(SslHandler handler) {
            return handler.engine.getSession().getPacketBufferSize();
        }

        abstract SSLEngineResult unwrap(SslHandler handler, ByteBuf in, int readerIndex, int len, ByteBuf out)
                throws SSLException;

        abstract int calculateWrapBufferCapacity(SslHandler handler, int pendingBytes, int numComponents);

        abstract int calculatePendingData(SslHandler handler, int guess);

        // BEGIN Platform-dependent flags

        /**
         * {@code true} if and only if {@link SSLEngine} expects a direct buffer.
         */
        final boolean wantsDirectBuffer;

        // END Platform-dependent flags

        /**
         * When using JDK {@link SSLEngine}, we use {@link #MERGE_CUMULATOR} because it works only with
         * one {@link ByteBuffer}.
         *
         * When using {@link OpenSslEngine}, we can use {@link #COMPOSITE_CUMULATOR} because it has
         * {@link OpenSslEngine#unwrap(ByteBuffer[], ByteBuffer[])} which works with multiple {@link ByteBuffer}s
         * and which does not need to do extra memory copies.
         */
        final Cumulator cumulator;
    }

    private volatile ChannelHandlerContext ctx;
    private final SSLEngine engine;
    private final SslEngineType engineType;
    private final Executor delegatedTaskExecutor;
    private final boolean jdkCompatibilityMode;

    /**
     * Used if {@link SSLEngine#wrap(ByteBuffer[], ByteBuffer)} and {@link SSLEngine#unwrap(ByteBuffer, ByteBuffer[])}
     * should be called with a {@link ByteBuf} that is only backed by one {@link ByteBuffer} to reduce the object
     * creation.
     */
    private final ByteBuffer[] singleBuffer = new ByteBuffer[1];

    private final boolean startTls;
    private boolean sentFirstMessage;
    private boolean flushedBeforeHandshake;
    private boolean readDuringHandshake;
    private final SslHandlerCoalescingBufferQueue pendingUnencryptedWrites = new SslHandlerCoalescingBufferQueue(16);
    private Promise<Channel> handshakePromise = new LazyChannelPromise();
    private final LazyChannelPromise sslClosePromise = new LazyChannelPromise();

    /**
     * Set by wrap*() methods when something is produced.
     * {@link #channelReadComplete(ChannelHandlerContext)} will check this flag, clear it, and call ctx.flush().
     */
    private boolean needsFlush;

    private boolean outboundClosed;

    private int packetLength;

    /**
     * This flag is used to determine if we need to call {@link ChannelHandlerContext#read()} to consume more data
     * when {@link ChannelConfig#isAutoRead()} is {@code false}.
     */
    private boolean firedChannelRead;

    private volatile long handshakeTimeoutMillis = 10000;
    private volatile long closeNotifyFlushTimeoutMillis = 3000;
    private volatile long closeNotifyReadTimeoutMillis;

    /**
     * Creates a new instance.
     *
     * @param engine  the {@link SSLEngine} this handler will use
     */
    public SslHandler(SSLEngine engine) {
        this(engine, false);
    }

    /**
     * Creates a new instance.
     *
     * @param engine    the {@link SSLEngine} this handler will use
     * @param startTls  {@code true} if the first write request shouldn't be
     *                  encrypted by the {@link SSLEngine}
     */
    @SuppressWarnings("deprecation")
    public SslHandler(SSLEngine engine, boolean startTls) {
        this(engine, startTls, ImmediateExecutor.INSTANCE);
    }

    /**
     * @deprecated Use {@link #SslHandler(SSLEngine)} instead.
     */
    @Deprecated
    public SslHandler(SSLEngine engine, Executor delegatedTaskExecutor) {
        this(engine, false, delegatedTaskExecutor);
    }

    /**
     * @deprecated Use {@link #SslHandler(SSLEngine, boolean)} instead.
     */
    @Deprecated
    public SslHandler(SSLEngine engine, boolean startTls, Executor delegatedTaskExecutor) {
        this(engine, startTls, true, delegatedTaskExecutor);
    }

    SslHandler(SSLEngine engine, boolean startTls, boolean jdkCompatibilityMode) {
        this(engine, startTls, jdkCompatibilityMode, ImmediateExecutor.INSTANCE);
    }

    SslHandler(SSLEngine engine, boolean startTls, boolean jdkCompatibilityMode, Executor delegatedTaskExecutor) {
        if (engine == null) {
            throw new NullPointerException("engine");
        }
        if (delegatedTaskExecutor == null) {
            throw new NullPointerException("delegatedTaskExecutor");
        }
        this.engine = engine;
        engineType = SslEngineType.forEngine(engine);
        this.delegatedTaskExecutor = delegatedTaskExecutor;
        this.startTls = startTls;
        this.jdkCompatibilityMode = jdkCompatibilityMode;
        setCumulator(engineType.cumulator);
    }

    public long getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    public void setHandshakeTimeout(long handshakeTimeout, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        setHandshakeTimeoutMillis(unit.toMillis(handshakeTimeout));
    }

    public void setHandshakeTimeoutMillis(long handshakeTimeoutMillis) {
        if (handshakeTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "handshakeTimeoutMillis: " + handshakeTimeoutMillis + " (expected: >= 0)");
        }
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
    }

    /**
     * Sets the number of bytes to pass to each {@link SSLEngine#wrap(ByteBuffer[], int, int, ByteBuffer)} call.
     * <p>
     * This value will partition data which is passed to write
     * {@link #write(ChannelHandlerContext, Object, ChannelPromise)}. The partitioning will work as follows:
     * <ul>
     * <li>If {@code wrapDataSize <= 0} then we will write each data chunk as is.</li>
     * <li>If {@code wrapDataSize > data size} then we will attempt to aggregate multiple data chunks together.</li>
     * <li>If {@code wrapDataSize > data size}  Else if {@code wrapDataSize <= data size} then we will divide the data
     * into chunks of {@code wrapDataSize} when writing.</li>
     * </ul>
     * <p>
     * If the {@link SSLEngine} doesn't support a gather wrap operation (e.g. {@link SslProvider#OPENSSL}) then
     * aggregating data before wrapping can help reduce the ratio between TLS overhead vs data payload which will lead
     * to better goodput. Writing fixed chunks of data can also help target the underlying transport's (e.g. TCP)
     * frame size. Under lossy/congested network conditions this may help the peer get full TLS packets earlier and
     * be able to do work sooner, as opposed to waiting for the all the pieces of the TLS packet to arrive.
     * @param wrapDataSize the number of bytes which will be passed to each
     *      {@link SSLEngine#wrap(ByteBuffer[], int, int, ByteBuffer)} call.
     */
    @UnstableApi
    public final void setWrapDataSize(int wrapDataSize) {
        pendingUnencryptedWrites.wrapDataSize = wrapDataSize;
    }

    /**
     * @deprecated use {@link #getCloseNotifyFlushTimeoutMillis()}
     */
    @Deprecated
    public long getCloseNotifyTimeoutMillis() {
        return getCloseNotifyFlushTimeoutMillis();
    }

    /**
     * @deprecated use {@link #setCloseNotifyFlushTimeout(long, TimeUnit)}
     */
    @Deprecated
    public void setCloseNotifyTimeout(long closeNotifyTimeout, TimeUnit unit) {
        setCloseNotifyFlushTimeout(closeNotifyTimeout, unit);
    }

    /**
     * @deprecated use {@link #setCloseNotifyFlushTimeoutMillis(long)}
     */
    @Deprecated
    public void setCloseNotifyTimeoutMillis(long closeNotifyFlushTimeoutMillis) {
        setCloseNotifyFlushTimeoutMillis(closeNotifyFlushTimeoutMillis);
    }

    /**
     * Gets the timeout for flushing the close_notify that was triggered by closing the
     * {@link Channel}. If the close_notify was not flushed in the given timeout the {@link Channel} will be closed
     * forcibly.
     */
    public final long getCloseNotifyFlushTimeoutMillis() {
        return closeNotifyFlushTimeoutMillis;
    }

    /**
     * Sets the timeout for flushing the close_notify that was triggered by closing the
     * {@link Channel}. If the close_notify was not flushed in the given timeout the {@link Channel} will be closed
     * forcibly.
     */
    public final void setCloseNotifyFlushTimeout(long closeNotifyFlushTimeout, TimeUnit unit) {
        setCloseNotifyFlushTimeoutMillis(unit.toMillis(closeNotifyFlushTimeout));
    }

    /**
     * See {@link #setCloseNotifyFlushTimeout(long, TimeUnit)}.
     */
    public final void setCloseNotifyFlushTimeoutMillis(long closeNotifyFlushTimeoutMillis) {
        if (closeNotifyFlushTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "closeNotifyFlushTimeoutMillis: " + closeNotifyFlushTimeoutMillis + " (expected: >= 0)");
        }
        this.closeNotifyFlushTimeoutMillis = closeNotifyFlushTimeoutMillis;
    }

    /**
     * Gets the timeout (in ms) for receiving the response for the close_notify that was triggered by closing the
     * {@link Channel}. This timeout starts after the close_notify message was successfully written to the
     * remote peer. Use {@code 0} to directly close the {@link Channel} and not wait for the response.
     */
    public final long getCloseNotifyReadTimeoutMillis() {
        return closeNotifyReadTimeoutMillis;
    }

    /**
     * Sets the timeout  for receiving the response for the close_notify that was triggered by closing the
     * {@link Channel}. This timeout starts after the close_notify message was successfully written to the
     * remote peer. Use {@code 0} to directly close the {@link Channel} and not wait for the response.
     */
    public final void setCloseNotifyReadTimeout(long closeNotifyReadTimeout, TimeUnit unit) {
        setCloseNotifyReadTimeoutMillis(unit.toMillis(closeNotifyReadTimeout));
    }

    /**
     * See {@link #setCloseNotifyReadTimeout(long, TimeUnit)}.
     */
    public final void setCloseNotifyReadTimeoutMillis(long closeNotifyReadTimeoutMillis) {
        if (closeNotifyReadTimeoutMillis < 0) {
            throw new IllegalArgumentException(
                    "closeNotifyReadTimeoutMillis: " + closeNotifyReadTimeoutMillis + " (expected: >= 0)");
        }
        this.closeNotifyReadTimeoutMillis = closeNotifyReadTimeoutMillis;
    }

    /**
     * Returns the {@link SSLEngine} which is used by this handler.
     */
    public SSLEngine engine() {
        return engine;
    }

    /**
     * Returns the name of the current application-level protocol.
     *
     * @return the protocol name or {@code null} if application-level protocol has not been negotiated
     */
    public String applicationProtocol() {
        SSLEngine engine = engine();
        if (!(engine instanceof ApplicationProtocolAccessor)) {
            return null;
        }

        return ((ApplicationProtocolAccessor) engine).getApplicationProtocol();
    }

    /**
     * Returns a {@link Future} that will get notified once the current TLS handshake completes.
     *
     * @return the {@link Future} for the initial TLS handshake if {@link #renegotiate()} was not invoked.
     *         The {@link Future} for the most recent {@linkplain #renegotiate() TLS renegotiation} otherwise.
     */
    public Future<Channel> handshakeFuture() {
        return handshakePromise;
    }

    /**
     * Sends an SSL {@code close_notify} message to the specified channel and
     * destroys the underlying {@link SSLEngine}.
     *
     * @deprecated use {@link Channel#close()} or {@link ChannelHandlerContext#close()}
     */
    @Deprecated
    public ChannelFuture close() {
        return close(ctx.newPromise());
    }

    /**
     * See {@link #close()}
     *
     * @deprecated use {@link Channel#close()} or {@link ChannelHandlerContext#close()}
     */
    @Deprecated
    public ChannelFuture close(final ChannelPromise promise) {
        final ChannelHandlerContext ctx = this.ctx;
        ctx.executor().execute(new Runnable() {
            @Override
            public void run() {
                outboundClosed = true;
                engine.closeOutbound();
                try {
                    flush(ctx, promise);
                } catch (Exception e) {
                    if (!promise.tryFailure(e)) {
                        logger.warn("{} flush() raised a masked exception.", ctx.channel(), e);
                    }
                }
            }
        });

        return promise;
    }

    /**
     * Return the {@link Future} that will get notified if the inbound of the {@link SSLEngine} is closed.
     *
     * This method will return the same {@link Future} all the time.
     *
     * @see SSLEngine
     */
    public Future<Channel> sslCloseFuture() {
        return sslClosePromise;
    }

    @Override
    public void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        if (!pendingUnencryptedWrites.isEmpty()) {
            // Check if queue is not empty first because create a new ChannelException is expensive
            pendingUnencryptedWrites.releaseAndFailAll(ctx,
                    new ChannelException("Pending write on removal of SslHandler"));
        }
        if (engine instanceof ReferenceCounted) {
            ((ReferenceCounted) engine).release();
        }
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                        ChannelPromise promise) throws Exception {
        ctx.connect(remoteAddress, localAddress, promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.deregister(promise);
    }

    @Override
    public void disconnect(final ChannelHandlerContext ctx,
                           final ChannelPromise promise) throws Exception {
        closeOutboundAndChannel(ctx, promise, true);
    }

    @Override
    public void close(final ChannelHandlerContext ctx,
                      final ChannelPromise promise) throws Exception {
        closeOutboundAndChannel(ctx, promise, false);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        if (!handshakePromise.isDone()) {
            readDuringHandshake = true;
        }

        ctx.read();
    }

    @Override
    public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            promise.setFailure(new UnsupportedMessageTypeException(msg, ByteBuf.class));
            return;
        }
        pendingUnencryptedWrites.add((ByteBuf) msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        // Do not encrypt the first write request if this handler is
        // created with startTLS flag turned on.
        if (startTls && !sentFirstMessage) {
            sentFirstMessage = true;
            pendingUnencryptedWrites.writeAndRemoveAll(ctx);
            forceFlush(ctx);
            return;
        }

        try {
            wrapAndFlush(ctx);
        } catch (Throwable cause) {
            setHandshakeFailure(ctx, cause);
            PlatformDependent.throwException(cause);
        }
    }

    private void wrapAndFlush(ChannelHandlerContext ctx) throws SSLException {
        if (pendingUnencryptedWrites.isEmpty()) {
            // It's important to NOT use a voidPromise here as the user
            // may want to add a ChannelFutureListener to the ChannelPromise later.
            //
            // See https://github.com/netty/netty/issues/3364
            pendingUnencryptedWrites.add(Unpooled.EMPTY_BUFFER, ctx.newPromise());
        }
        if (!handshakePromise.isDone()) {
            flushedBeforeHandshake = true;
        }
        try {
            wrap(ctx, false);
        } finally {
            // We may have written some parts of data before an exception was thrown so ensure we always flush.
            // See https://github.com/netty/netty/issues/3900#issuecomment-172481830
            forceFlush(ctx);
        }
    }

    // This method will not call setHandshakeFailure(...) !
    private void wrap(ChannelHandlerContext ctx, boolean inUnwrap) throws SSLException {
        ByteBuf out = null;
        ChannelPromise promise = null;
        ByteBufAllocator alloc = ctx.alloc();
        boolean needUnwrap = false;
        try {
            final int wrapDataSize = pendingUnencryptedWrites.wrapDataSize;
            // Only continue to loop if the handler was not removed in the meantime.
            // See https://github.com/netty/netty/issues/5860
            while (!ctx.isRemoved()) {
                promise = ctx.newPromise();
                ByteBuf buf = wrapDataSize > 0 ?
                        pendingUnencryptedWrites.remove(alloc, wrapDataSize, promise) :
                        pendingUnencryptedWrites.removeFirst(promise);
                if (buf == null) {
                    break;
                }

                if (out == null) {
                    out = allocateOutNetBuf(ctx, buf.readableBytes(), buf.nioBufferCount());
                }

                SSLEngineResult result = wrap(alloc, engine, buf, out);

                if (result.getStatus() == Status.CLOSED) {
                    buf.release();
                    promise.tryFailure(SSLENGINE_CLOSED);
                    promise = null;
                    // SSLEngine has been closed already.
                    // Any further write attempts should be denied.
                    pendingUnencryptedWrites.releaseAndFailAll(ctx, SSLENGINE_CLOSED);
                    return;
                } else {
                    if (buf.isReadable()) {
                        pendingUnencryptedWrites.addFirst(buf);
                    } else {
                        buf.release();
                    }

                    switch (result.getHandshakeStatus()) {
                        case NEED_TASK:
                            runDelegatedTasks();
                            break;
                        case FINISHED:
                            setHandshakeSuccess();
                            // deliberate fall-through
                        case NOT_HANDSHAKING:
                            setHandshakeSuccessIfStillHandshaking();
                            // deliberate fall-through
                        case NEED_WRAP:
                            finishWrap(ctx, out, promise, inUnwrap, false);
                            promise = null;
                            out = null;
                            break;
                        case NEED_UNWRAP:
                            needUnwrap = true;
                            return;
                        default:
                            throw new IllegalStateException(
                                    "Unknown handshake status: " + result.getHandshakeStatus());
                    }
                }
            }
        } finally {
            finishWrap(ctx, out, promise, inUnwrap, needUnwrap);
        }
    }

    private void finishWrap(ChannelHandlerContext ctx, ByteBuf out, ChannelPromise promise, boolean inUnwrap,
            boolean needUnwrap) {
        if (out == null) {
            out = Unpooled.EMPTY_BUFFER;
        } else if (!out.isReadable()) {
            out.release();
            out = Unpooled.EMPTY_BUFFER;
        }

        if (promise != null) {
            ctx.write(out, promise);
        } else {
            ctx.write(out);
        }

        if (inUnwrap) {
            needsFlush = true;
        }

        if (needUnwrap) {
            // The underlying engine is starving so we need to feed it with more data.
            // See https://github.com/netty/netty/pull/5039
            readIfNeeded(ctx);
        }
    }

    /**
     * This method will not call
     * {@link #setHandshakeFailure(ChannelHandlerContext, Throwable, boolean)} or
     * {@link #setHandshakeFailure(ChannelHandlerContext, Throwable)}.
     * @return {@code true} if this method ends on {@link SSLEngineResult.HandshakeStatus#NOT_HANDSHAKING}.
     */
    private boolean wrapNonAppData(ChannelHandlerContext ctx, boolean inUnwrap) throws SSLException {
        ByteBuf out = null;
        ByteBufAllocator alloc = ctx.alloc();
        try {
            // Only continue to loop if the handler was not removed in the meantime.
            // See https://github.com/netty/netty/issues/5860
            while (!ctx.isRemoved()) {
                if (out == null) {
                    // As this is called for the handshake we have no real idea how big the buffer needs to be.
                    // That said 2048 should give us enough room to include everything like ALPN / NPN data.
                    // If this is not enough we will increase the buffer in wrap(...).
                    out = allocateOutNetBuf(ctx, 2048, 1);
                }
                SSLEngineResult result = wrap(alloc, engine, Unpooled.EMPTY_BUFFER, out);

                if (result.bytesProduced() > 0) {
                    ctx.write(out);
                    if (inUnwrap) {
                        needsFlush = true;
                    }
                    out = null;
                }

                switch (result.getHandshakeStatus()) {
                    case FINISHED:
                        setHandshakeSuccess();
                        return false;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case NEED_UNWRAP:
                        if (inUnwrap) {
                            // If we asked for a wrap, the engine requested an unwrap, and we are in unwrap there is
                            // no use in trying to call wrap again because we have already attempted (or will after we
                            // return) to feed more data to the engine.
                            return false;
                        }

                        unwrapNonAppData(ctx);
                        break;
                    case NEED_WRAP:
                        break;
                    case NOT_HANDSHAKING:
                        setHandshakeSuccessIfStillHandshaking();
                        // Workaround for TLS False Start problem reported at:
                        // https://github.com/netty/netty/issues/1108#issuecomment-14266970
                        if (!inUnwrap) {
                            unwrapNonAppData(ctx);
                        }
                        return true;
                    default:
                        throw new IllegalStateException("Unknown handshake status: " + result.getHandshakeStatus());
                }

                if (result.bytesProduced() == 0) {
                    break;
                }

                // It should not consume empty buffers when it is not handshaking
                // Fix for Android, where it was encrypting empty buffers even when not handshaking
                if (result.bytesConsumed() == 0 && result.getHandshakeStatus() == HandshakeStatus.NOT_HANDSHAKING) {
                    break;
                }
            }
        }  finally {
            if (out != null) {
                out.release();
            }
        }
        return false;
    }

    private SSLEngineResult wrap(ByteBufAllocator alloc, SSLEngine engine, ByteBuf in, ByteBuf out)
            throws SSLException {
        ByteBuf newDirectIn = null;
        try {
            int readerIndex = in.readerIndex();
            int readableBytes = in.readableBytes();

            // We will call SslEngine.wrap(ByteBuffer[], ByteBuffer) to allow efficient handling of
            // CompositeByteBuf without force an extra memory copy when CompositeByteBuffer.nioBuffer() is called.
            final ByteBuffer[] in0;
            if (in.isDirect() || !engineType.wantsDirectBuffer) {
                // As CompositeByteBuf.nioBufferCount() can be expensive (as it needs to check all composed ByteBuf
                // to calculate the count) we will just assume a CompositeByteBuf contains more then 1 ByteBuf.
                // The worst that can happen is that we allocate an extra ByteBuffer[] in CompositeByteBuf.nioBuffers()
                // which is better then walking the composed ByteBuf in most cases.
                if (!(in instanceof CompositeByteBuf) && in.nioBufferCount() == 1) {
                    in0 = singleBuffer;
                    // We know its only backed by 1 ByteBuffer so use internalNioBuffer to keep object allocation
                    // to a minimum.
                    in0[0] = in.internalNioBuffer(readerIndex, readableBytes);
                } else {
                    in0 = in.nioBuffers();
                }
            } else {
                // We could even go further here and check if its a CompositeByteBuf and if so try to decompose it and
                // only replace the ByteBuffer that are not direct. At the moment we just will replace the whole
                // CompositeByteBuf to keep the complexity to a minimum
                newDirectIn = alloc.directBuffer(readableBytes);
                newDirectIn.writeBytes(in, readerIndex, readableBytes);
                in0 = singleBuffer;
                in0[0] = newDirectIn.internalNioBuffer(newDirectIn.readerIndex(), readableBytes);
            }

            for (;;) {
                ByteBuffer out0 = out.nioBuffer(out.writerIndex(), out.writableBytes());
                SSLEngineResult result = engine.wrap(in0, out0);
                in.skipBytes(result.bytesConsumed());
                out.writerIndex(out.writerIndex() + result.bytesProduced());

                switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    out.ensureWritable(engine.getSession().getPacketBufferSize());
                    break;
                default:
                    return result;
                }
            }
        } finally {
            // Null out to allow GC of ByteBuffer
            singleBuffer[0] = null;

            if (newDirectIn != null) {
                newDirectIn.release();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Make sure to release SSLEngine,
        // and notify the handshake future if the connection has been closed during handshake.
        setHandshakeFailure(ctx, CHANNEL_CLOSED, !outboundClosed);

        // Ensure we always notify the sslClosePromise as well
        notifyClosePromise(CHANNEL_CLOSED);

        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (ignoreException(cause)) {
            // It is safe to ignore the 'connection reset by peer' or
            // 'broken pipe' error after sending close_notify.
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{} Swallowing a harmless 'connection reset by peer / broken pipe' error that occurred " +
                        "while writing close_notify in response to the peer's close_notify", ctx.channel(), cause);
            }

            // Close the connection explicitly just in case the transport
            // did not close the connection automatically.
            if (ctx.channel().isActive()) {
                ctx.close();
            }
        } else {
            ctx.fireExceptionCaught(cause);
        }
    }

    /**
     * Checks if the given {@link Throwable} can be ignore and just "swallowed"
     *
     * When an ssl connection is closed a close_notify message is sent.
     * After that the peer also sends close_notify however, it's not mandatory to receive
     * the close_notify. The party who sent the initial close_notify can close the connection immediately
     * then the peer will get connection reset error.
     *
     */
    private boolean ignoreException(Throwable t) {
        if (!(t instanceof SSLException) && t instanceof IOException && sslClosePromise.isDone()) {
            String message = t.getMessage();

            // first try to match connection reset / broke peer based on the regex. This is the fastest way
            // but may fail on different jdk impls or OS's
            if (message != null && IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
                return true;
            }

            // Inspect the StackTraceElements to see if it was a connection reset / broken pipe or not
            StackTraceElement[] elements = t.getStackTrace();
            for (StackTraceElement element: elements) {
                String classname = element.getClassName();
                String methodname = element.getMethodName();

                // skip all classes that belong to the io.netty package
                if (classname.startsWith("io.netty.")) {
                    continue;
                }

                // check if the method name is read if not skip it
                if (!"read".equals(methodname)) {
                    continue;
                }

                // This will also match against SocketInputStream which is used by openjdk 7 and maybe
                // also others
                if (IGNORABLE_CLASS_IN_STACK.matcher(classname).matches()) {
                    return true;
                }

                try {
                    // No match by now.. Try to load the class via classloader and inspect it.
                    // This is mainly done as other JDK implementations may differ in name of
                    // the impl.
                    Class<?> clazz = PlatformDependent.getClassLoader(getClass()).loadClass(classname);

                    if (SocketChannel.class.isAssignableFrom(clazz)
                            || DatagramChannel.class.isAssignableFrom(clazz)) {
                        return true;
                    }

                    // also match against SctpChannel via String matching as it may not present.
                    if (PlatformDependent.javaVersion() >= 7
                            && "com.sun.nio.sctp.SctpChannel".equals(clazz.getSuperclass().getName())) {
                        return true;
                    }
                } catch (Throwable cause) {
                    logger.debug("Unexpected exception while loading class {} classname {}",
                                 getClass(), classname, cause);
                }
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if the given {@link ByteBuf} is encrypted. Be aware that this method
     * will not increase the readerIndex of the given {@link ByteBuf}.
     *
     * @param   buffer
     *                  The {@link ByteBuf} to read from. Be aware that it must have at least 5 bytes to read,
     *                  otherwise it will throw an {@link IllegalArgumentException}.
     * @return encrypted
     *                  {@code true} if the {@link ByteBuf} is encrypted, {@code false} otherwise.
     * @throws IllegalArgumentException
     *                  Is thrown if the given {@link ByteBuf} has not at least 5 bytes to read.
     */
    public static boolean isEncrypted(ByteBuf buffer) {
        if (buffer.readableBytes() < SslUtils.SSL_RECORD_HEADER_LENGTH) {
            throw new IllegalArgumentException(
                    "buffer must have at least " + SslUtils.SSL_RECORD_HEADER_LENGTH + " readable bytes");
        }
        return getEncryptedPacketLength(buffer, buffer.readerIndex()) != SslUtils.NOT_ENCRYPTED;
    }

    private void decodeJdkCompatible(ChannelHandlerContext ctx, ByteBuf in) throws NotSslRecordException {
        int packetLength = this.packetLength;
        // If we calculated the length of the current SSL record before, use that information.
        if (packetLength > 0) {
            if (in.readableBytes() < packetLength) {
                return;
            }
        } else {
            // Get the packet length and wait until we get a packets worth of data to unwrap.
            final int readableBytes = in.readableBytes();
            if (readableBytes < SslUtils.SSL_RECORD_HEADER_LENGTH) {
                return;
            }
            packetLength = getEncryptedPacketLength(in, in.readerIndex());
            if (packetLength == SslUtils.NOT_ENCRYPTED) {
                // Not an SSL/TLS packet
                NotSslRecordException e = new NotSslRecordException(
                        "not an SSL/TLS record: " + ByteBufUtil.hexDump(in));
                in.skipBytes(in.readableBytes());

                // First fail the handshake promise as we may need to have access to the SSLEngine which may
                // be released because the user will remove the SslHandler in an exceptionCaught(...) implementation.
                setHandshakeFailure(ctx, e);

                throw e;
            }
            assert packetLength > 0;
            if (packetLength > readableBytes) {
                // wait until the whole packet can be read
                this.packetLength = packetLength;
                return;
            }
        }

        // Reset the state of this class so we can get the length of the next packet. We assume the entire packet will
        // be consumed by the SSLEngine.
        this.packetLength = 0;
        try {
            int bytesConsumed = unwrap(ctx, in, in.readerIndex(), packetLength);
            assert bytesConsumed == packetLength || engine.isInboundDone() :
                    "we feed the SSLEngine a packets worth of data: " + packetLength + " but it only consumed: " +
                            bytesConsumed;
            in.skipBytes(bytesConsumed);
        } catch (Throwable cause) {
            handleUnwrapThrowable(ctx, cause);
        }
    }

    private void decodeNonJdkCompatible(ChannelHandlerContext ctx, ByteBuf in) {
        try {
            in.skipBytes(unwrap(ctx, in, in.readerIndex(), in.readableBytes()));
        } catch (Throwable cause) {
            handleUnwrapThrowable(ctx, cause);
        }
    }

    private void handleUnwrapThrowable(ChannelHandlerContext ctx, Throwable cause) {
        try {
            // We need to flush one time as there may be an alert that we should send to the remote peer because
            // of the SSLException reported here.
            wrapAndFlush(ctx);
        } catch (SSLException ex) {
            logger.debug("SSLException during trying to call SSLEngine.wrap(...)" +
                    " because of an previous SSLException, ignoring...", ex);
        } finally {
            setHandshakeFailure(ctx, cause);
        }
        PlatformDependent.throwException(cause);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws SSLException {
        if (jdkCompatibilityMode) {
            decodeJdkCompatible(ctx, in);
        } else {
            decodeNonJdkCompatible(ctx, in);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // Discard bytes of the cumulation buffer if needed.
        discardSomeReadBytes();

        flushIfNeeded(ctx);
        readIfNeeded(ctx);

        firedChannelRead = false;
        ctx.fireChannelReadComplete();
    }

    private void readIfNeeded(ChannelHandlerContext ctx) {
        // If handshake is not finished yet, we need more data.
        if (!ctx.channel().config().isAutoRead() && (!firedChannelRead || !handshakePromise.isDone())) {
            // No auto-read used and no message passed through the ChannelPipeline or the handshake was not complete
            // yet, which means we need to trigger the read to ensure we not encounter any stalls.
            ctx.read();
        }
    }

    private void flushIfNeeded(ChannelHandlerContext ctx) {
        if (needsFlush) {
            forceFlush(ctx);
        }
    }

    /**
     * Calls {@link SSLEngine#unwrap(ByteBuffer, ByteBuffer)} with an empty buffer to handle handshakes, etc.
     */
    private void unwrapNonAppData(ChannelHandlerContext ctx) throws SSLException {
        unwrap(ctx, Unpooled.EMPTY_BUFFER, 0, 0);
    }

    /**
     * Unwraps inbound SSL records.
     */
    private int unwrap(
            ChannelHandlerContext ctx, ByteBuf packet, int offset, int length) throws SSLException {
        final int originalLength = length;
        boolean wrapLater = false;
        boolean notifyClosure = false;
        ByteBuf decodeOut = allocate(ctx, length);
        try {
            // Only continue to loop if the handler was not removed in the meantime.
            // See https://github.com/netty/netty/issues/5860
            unwrapLoop: while (!ctx.isRemoved()) {
                final SSLEngineResult result = engineType.unwrap(this, packet, offset, length, decodeOut);
                final Status status = result.getStatus();
                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                final int produced = result.bytesProduced();
                final int consumed = result.bytesConsumed();

                // Update indexes for the next iteration
                offset += consumed;
                length -= consumed;

                switch (status) {
                case BUFFER_OVERFLOW:
                    int readableBytes = decodeOut.readableBytes();
                    int bufferSize = engine.getSession().getApplicationBufferSize() - readableBytes;
                    if (readableBytes > 0) {
                        firedChannelRead = true;
                        ctx.fireChannelRead(decodeOut);

                        // This buffer was handled, null it out.
                        decodeOut = null;
                        if (bufferSize <= 0) {
                            // It may happen that readableBytes >= engine.getSession().getApplicationBufferSize()
                            // while there is still more to unwrap, in this case we will just allocate a new buffer
                            // with the capacity of engine.getSession().getApplicationBufferSize() and call unwrap
                            // again.
                            bufferSize = engine.getSession().getApplicationBufferSize();
                        }
                    } else {
                        // This buffer was handled, null it out.
                        decodeOut.release();
                        decodeOut = null;
                    }
                    // Allocate a new buffer which can hold all the rest data and loop again.
                    // TODO: We may want to reconsider how we calculate the length here as we may
                    // have more then one ssl message to decode.
                    decodeOut = allocate(ctx, engineType.calculatePendingData(this, bufferSize));
                    continue;
                case CLOSED:
                    // notify about the CLOSED state of the SSLEngine. See #137
                    notifyClosure = true;
                    break;
                default:
                    break;
                }

                switch (handshakeStatus) {
                    case NEED_UNWRAP:
                        break;
                    case NEED_WRAP:
                        // If the wrap operation transitions the status to NOT_HANDSHAKING and there is no more data to
                        // unwrap then the next call to unwrap will not produce any data. We can avoid the potentially
                        // costly unwrap operation and break out of the loop.
                        if (wrapNonAppData(ctx, true) && length == 0) {
                            break unwrapLoop;
                        }
                        break;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case FINISHED:
                        setHandshakeSuccess();
                        wrapLater = true;

                        // We 'break' here and NOT 'continue' as android API version 21 has a bug where they consume
                        // data from the buffer but NOT correctly set the SSLEngineResult.bytesConsumed().
                        // Because of this it will raise an exception on the next iteration of the for loop on android
                        // API version 21. Just doing a break will work here as produced and consumed will both be 0
                        // and so we break out of the complete for (;;) loop and so call decode(...) again later on.
                        // On other platforms this will have no negative effect as we will just continue with the
                        // for (;;) loop if something was either consumed or produced.
                        //
                        // See:
                        //  - https://github.com/netty/netty/issues/4116
                        //  - https://code.google.com/p/android/issues/detail?id=198639&thanks=198639&ts=1452501203
                        break;
                    case NOT_HANDSHAKING:
                        if (setHandshakeSuccessIfStillHandshaking()) {
                            wrapLater = true;
                            continue;
                        }
                        if (flushedBeforeHandshake) {
                            // We need to call wrap(...) in case there was a flush done before the handshake completed.
                            //
                            // See https://github.com/netty/netty/pull/2437
                            flushedBeforeHandshake = false;
                            wrapLater = true;
                        }
                        // If we are not handshaking and there is no more data to unwrap then the next call to unwrap
                        // will not produce any data. We can avoid the potentially costly unwrap operation and break
                        // out of the loop.
                        if (length == 0) {
                            break unwrapLoop;
                        }
                        break;
                    default:
                        throw new IllegalStateException("unknown handshake status: " + handshakeStatus);
                }

                if (status == Status.BUFFER_UNDERFLOW || consumed == 0 && produced == 0) {
                    if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                        // The underlying engine is starving so we need to feed it with more data.
                        // See https://github.com/netty/netty/pull/5039
                        readIfNeeded(ctx);
                    }

                    break;
                }
            }

            if (wrapLater) {
                wrap(ctx, true);
            }

            if (notifyClosure) {
                notifyClosePromise(null);
            }
        } finally {
            if (decodeOut != null) {
                if (decodeOut.isReadable()) {
                    firedChannelRead = true;

                    ctx.fireChannelRead(decodeOut);
                } else {
                    decodeOut.release();
                }
            }
        }
        return originalLength - length;
    }

    private static ByteBuffer toByteBuffer(ByteBuf out, int index, int len) {
        return out.nioBufferCount() == 1 ? out.internalNioBuffer(index, len) :
                out.nioBuffer(index, len);
    }

    /**
     * Fetches all delegated tasks from the {@link SSLEngine} and runs them via the {@link #delegatedTaskExecutor}.
     * If the {@link #delegatedTaskExecutor} is {@link ImmediateExecutor}, just call {@link Runnable#run()} directly
     * instead of using {@link Executor#execute(Runnable)}.  Otherwise, run the tasks via
     * the {@link #delegatedTaskExecutor} and wait until the tasks are finished.
     */
    private void runDelegatedTasks() {
        if (delegatedTaskExecutor == ImmediateExecutor.INSTANCE) {
            for (;;) {
                Runnable task = engine.getDelegatedTask();
                if (task == null) {
                    break;
                }

                task.run();
            }
        } else {
            final List<Runnable> tasks = new ArrayList<Runnable>(2);
            for (;;) {
                final Runnable task = engine.getDelegatedTask();
                if (task == null) {
                    break;
                }

                tasks.add(task);
            }

            if (tasks.isEmpty()) {
                return;
            }

            final CountDownLatch latch = new CountDownLatch(1);
            delegatedTaskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (Runnable task: tasks) {
                            task.run();
                        }
                    } catch (Exception e) {
                        ctx.fireExceptionCaught(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });

            boolean interrupted = false;
            while (latch.getCount() != 0) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    // Interrupt later.
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Works around some Android {@link SSLEngine} implementations that skip {@link HandshakeStatus#FINISHED} and
     * go straight into {@link HandshakeStatus#NOT_HANDSHAKING} when handshake is finished.
     *
     * @return {@code true} if and only if the workaround has been applied and thus {@link #handshakeFuture} has been
     *         marked as success by this method
     */
    private boolean setHandshakeSuccessIfStillHandshaking() {
        if (!handshakePromise.isDone()) {
            setHandshakeSuccess();
            return true;
        }
        return false;
    }

    /**
     * Notify all the handshake futures about the successfully handshake
     */
    private void setHandshakeSuccess() {
        handshakePromise.trySuccess(ctx.channel());

        if (logger.isDebugEnabled()) {
            logger.debug("{} HANDSHAKEN: {}", ctx.channel(), engine.getSession().getCipherSuite());
        }
        ctx.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);

        if (readDuringHandshake && !ctx.channel().config().isAutoRead()) {
            readDuringHandshake = false;
            ctx.read();
        }
    }

    /**
     * Notify all the handshake futures about the failure during the handshake.
     */
    private void setHandshakeFailure(ChannelHandlerContext ctx, Throwable cause) {
        setHandshakeFailure(ctx, cause, true);
    }

    /**
     * Notify all the handshake futures about the failure during the handshake.
     */
    private void setHandshakeFailure(ChannelHandlerContext ctx, Throwable cause, boolean closeInbound) {
        try {
            // Release all resources such as internal buffers that SSLEngine
            // is managing.
            engine.closeOutbound();

            if (closeInbound) {
                try {
                    engine.closeInbound();
                } catch (SSLException e) {
                    // only log in debug mode as it most likely harmless and latest chrome still trigger
                    // this all the time.
                    //
                    // See https://github.com/netty/netty/issues/1340
                    String msg = e.getMessage();
                    if (msg == null || !msg.contains("possible truncation attack")) {
                        logger.debug("{} SSLEngine.closeInbound() raised an exception.", ctx.channel(), e);
                    }
                }
            }
            notifyHandshakeFailure(cause);
        } finally {
            // Ensure we remove and fail all pending writes in all cases and so release memory quickly.
            pendingUnencryptedWrites.releaseAndFailAll(ctx, cause);
        }
    }

    private void notifyHandshakeFailure(Throwable cause) {
        if (handshakePromise.tryFailure(cause)) {
            SslUtils.notifyHandshakeFailure(ctx, cause);
        }
    }

    private void notifyClosePromise(Throwable cause) {
        if (cause == null) {
            if (sslClosePromise.trySuccess(ctx.channel())) {
                ctx.fireUserEventTriggered(SslCloseCompletionEvent.SUCCESS);
            }
        } else {
            if (sslClosePromise.tryFailure(cause)) {
                ctx.fireUserEventTriggered(new SslCloseCompletionEvent(cause));
            }
        }
    }

    private void closeOutboundAndChannel(
            final ChannelHandlerContext ctx, final ChannelPromise promise, boolean disconnect) throws Exception {
        if (!ctx.channel().isActive()) {
            if (disconnect) {
                ctx.disconnect(promise);
            } else {
                ctx.close(promise);
            }
            return;
        }

        outboundClosed = true;
        engine.closeOutbound();

        ChannelPromise closeNotifyPromise = ctx.newPromise();
        try {
            flush(ctx, closeNotifyPromise);
        } finally {
            // It's important that we do not pass the original ChannelPromise to safeClose(...) as when flush(....)
            // throws an Exception it will be propagated to the AbstractChannelHandlerContext which will try
            // to fail the promise because of this. This will then fail as it was already completed by safeClose(...).
            // We create a new ChannelPromise and try to notify the original ChannelPromise
            // once it is complete. If we fail to do so we just ignore it as in this case it was failed already
            // because of a propagated Exception.
            //
            // See https://github.com/netty/netty/issues/5931
            safeClose(ctx, closeNotifyPromise, ctx.newPromise().addListener(
                    new ChannelPromiseNotifier(false, promise)));
        }
    }

    private void flush(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        pendingUnencryptedWrites.add(Unpooled.EMPTY_BUFFER, promise);
        flush(ctx);
    }

    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;

        if (ctx.channel().isActive() && engine.getUseClientMode()) {
            // Begin the initial handshake.
            // channelActive() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            handshake(null);
        } else {
            // channelActive() event has not been fired yet.  this.channelOpen() will be invoked
            // and initialization will occur there.
        }
    }

    /**
     * Performs TLS renegotiation.
     */
    public Future<Channel> renegotiate() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            throw new IllegalStateException();
        }

        return renegotiate(ctx.executor().<Channel>newPromise());
    }

    /**
     * Performs TLS renegotiation.
     */
    public Future<Channel> renegotiate(final Promise<Channel> promise) {
        if (promise == null) {
            throw new NullPointerException("promise");
        }

        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            throw new IllegalStateException();
        }

        EventExecutor executor = ctx.executor();
        if (!executor.inEventLoop()) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    handshake(promise);
                }
            });
            return promise;
        }

        handshake(promise);
        return promise;
    }

    /**
     * Performs TLS (re)negotiation.
     *
     * @param newHandshakePromise if {@code null}, use the existing {@link #handshakePromise},
     *                            assuming that the current negotiation has not been finished.
     *                            Currently, {@code null} is expected only for the initial handshake.
     */
    private void handshake(final Promise<Channel> newHandshakePromise) {
        final Promise<Channel> p;
        if (newHandshakePromise != null) {
            final Promise<Channel> oldHandshakePromise = handshakePromise;
            if (!oldHandshakePromise.isDone()) {
                // There's no need to handshake because handshake is in progress already.
                // Merge the new promise into the old one.
                oldHandshakePromise.addListener(new FutureListener<Channel>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
                        if (future.isSuccess()) {
                            newHandshakePromise.setSuccess(future.getNow());
                        } else {
                            newHandshakePromise.setFailure(future.cause());
                        }
                    }
                });
                return;
            }

            handshakePromise = p = newHandshakePromise;
        } else if (engine.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING) {
            // Not all SSLEngine implementations support calling beginHandshake multiple times while a handshake
            // is in progress. See https://github.com/netty/netty/issues/4718.
            return;
        } else {
            // Forced to reuse the old handshake.
            p = handshakePromise;
            assert !p.isDone();
        }

        // Begin handshake.
        final ChannelHandlerContext ctx = this.ctx;
        try {
            engine.beginHandshake();
            wrapNonAppData(ctx, false);
        } catch (Throwable e) {
            setHandshakeFailure(ctx, e);
        } finally {
           forceFlush(ctx);
        }

        // Set timeout if necessary.
        final long handshakeTimeoutMillis = this.handshakeTimeoutMillis;
        if (handshakeTimeoutMillis <= 0 || p.isDone()) {
            return;
        }

        final ScheduledFuture<?> timeoutFuture = ctx.executor().schedule(new Runnable() {
            @Override
            public void run() {
                if (p.isDone()) {
                    return;
                }
                notifyHandshakeFailure(HANDSHAKE_TIMED_OUT);
            }
        }, handshakeTimeoutMillis, TimeUnit.MILLISECONDS);

        // Cancel the handshake timeout when handshake is finished.
        p.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> f) throws Exception {
                timeoutFuture.cancel(false);
            }
        });
    }

    private void forceFlush(ChannelHandlerContext ctx) {
        needsFlush = false;
        ctx.flush();
    }

    /**
     * Issues an initial TLS handshake once connected when used in client-mode
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (!startTls && engine.getUseClientMode()) {
            // Begin the initial handshake
            handshake(null);
        }
        ctx.fireChannelActive();
    }

    private void safeClose(
            final ChannelHandlerContext ctx, final ChannelFuture flushFuture,
            final ChannelPromise promise) {
        if (!ctx.channel().isActive()) {
            ctx.close(promise);
            return;
        }

        final ScheduledFuture<?> timeoutFuture;
        if (!flushFuture.isDone()) {
            long closeNotifyTimeout = closeNotifyFlushTimeoutMillis;
            if (closeNotifyTimeout > 0) {
                // Force-close the connection if close_notify is not fully sent in time.
                timeoutFuture = ctx.executor().schedule(new Runnable() {
                    @Override
                    public void run() {
                        // May be done in the meantime as cancel(...) is only best effort.
                        if (!flushFuture.isDone()) {
                            logger.warn("{} Last write attempt timed out; force-closing the connection.",
                                    ctx.channel());
                            addCloseListener(ctx.close(ctx.newPromise()), promise);
                        }
                    }
                }, closeNotifyTimeout, TimeUnit.MILLISECONDS);
            } else {
                timeoutFuture = null;
            }
        } else {
            timeoutFuture = null;
        }

        // Close the connection if close_notify is sent in time.
        flushFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f)
                    throws Exception {
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(false);
                }
                final long closeNotifyReadTimeout = closeNotifyReadTimeoutMillis;
                if (closeNotifyReadTimeout <= 0) {
                    // Trigger the close in all cases to make sure the promise is notified
                    // See https://github.com/netty/netty/issues/2358
                    addCloseListener(ctx.close(ctx.newPromise()), promise);
                } else {
                    final ScheduledFuture<?> closeNotifyReadTimeoutFuture;

                    if (!sslClosePromise.isDone()) {
                        closeNotifyReadTimeoutFuture = ctx.executor().schedule(new Runnable() {
                            @Override
                            public void run() {
                                if (!sslClosePromise.isDone()) {
                                    logger.debug(
                                            "{} did not receive close_notify in {}ms; force-closing the connection.",
                                            ctx.channel(), closeNotifyReadTimeout);

                                    // Do the close now...
                                    addCloseListener(ctx.close(ctx.newPromise()), promise);
                                }
                            }
                        }, closeNotifyReadTimeout, TimeUnit.MILLISECONDS);
                    } else {
                        closeNotifyReadTimeoutFuture = null;
                    }

                    // Do the close once the we received the close_notify.
                    sslClosePromise.addListener(new FutureListener<Channel>() {
                        @Override
                        public void operationComplete(Future<Channel> future) throws Exception {
                            if (closeNotifyReadTimeoutFuture != null) {
                                closeNotifyReadTimeoutFuture.cancel(false);
                            }
                            addCloseListener(ctx.close(ctx.newPromise()), promise);
                        }
                    });
                }
            }
        });
    }

    private static void addCloseListener(ChannelFuture future, ChannelPromise promise) {
        // We notify the promise in the ChannelPromiseNotifier as there is a "race" where the close(...) call
        // by the timeoutFuture and the close call in the flushFuture listener will be called. Because of
        // this we need to use trySuccess() and tryFailure(...) as otherwise we can cause an
        // IllegalStateException.
        // Also we not want to log if the notification happens as this is expected in some cases.
        // See https://github.com/netty/netty/issues/5598
        future.addListener(new ChannelPromiseNotifier(false, promise));
    }

    /**
     * Always prefer a direct buffer when it's pooled, so that we reduce the number of memory copies
     * in {@link OpenSslEngine}.
     */
    private ByteBuf allocate(ChannelHandlerContext ctx, int capacity) {
        ByteBufAllocator alloc = ctx.alloc();
        if (engineType.wantsDirectBuffer) {
            return alloc.directBuffer(capacity);
        } else {
            return alloc.buffer(capacity);
        }
    }

    /**
     * Allocates an outbound network buffer for {@link SSLEngine#wrap(ByteBuffer, ByteBuffer)} which can encrypt
     * the specified amount of pending bytes.
     */
    private ByteBuf allocateOutNetBuf(ChannelHandlerContext ctx, int pendingBytes, int numComponents) {
        return allocate(ctx, engineType.calculateWrapBufferCapacity(this, pendingBytes, numComponents));
    }

    /**
     * Each call to SSL_write will introduce about ~100 bytes of overhead. This coalescing queue attempts to increase
     * goodput by aggregating the plaintext in chunks of {@link #wrapDataSize}. If many small chunks are written
     * this can increase goodput, decrease the amount of calls to SSL_write, and decrease overall encryption operations.
     */
    private static final class SslHandlerCoalescingBufferQueue extends AbstractCoalescingBufferQueue {
        volatile int wrapDataSize = MAX_PLAINTEXT_LENGTH;

        SslHandlerCoalescingBufferQueue(int initSize) {
            super(initSize);
        }

        @Override
        protected ByteBuf compose(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf next) {
            final int wrapDataSize = this.wrapDataSize;
            if (cumulation instanceof CompositeByteBuf) {
                CompositeByteBuf composite = (CompositeByteBuf) cumulation;
                int numComponents = composite.numComponents();
                if (numComponents == 0 ||
                        !attemptCopyToCumulation(composite.internalComponent(numComponents - 1), next, wrapDataSize)) {
                    composite.addComponent(true, next);
                }
                return composite;
            }
            if (attemptCopyToCumulation(cumulation, next, wrapDataSize)) {
                return cumulation;
            }
            CompositeByteBuf composite = alloc.compositeDirectBuffer(size() + 2);
            composite.addComponent(true, cumulation);
            composite.addComponent(true, next);
            return composite;
        }

        @Override
        protected ByteBuf composeFirst(ByteBufAllocator allocator, ByteBuf first) {
            if (first instanceof CompositeByteBuf) {
                CompositeByteBuf composite = (CompositeByteBuf) first;
                first = allocator.directBuffer(composite.readableBytes());
                first.writeBytes(composite);
                composite.release();
            }
            return first;
        }

        @Override
        protected ByteBuf removeEmptyValue() {
            return null;
        }

        private static boolean attemptCopyToCumulation(ByteBuf cumulation, ByteBuf next, int wrapDataSize) {
            final int inReadableBytes = next.readableBytes();
            final int cumulationCapacity = cumulation.capacity();
            if (wrapDataSize - cumulation.readableBytes() >= inReadableBytes &&
                    // Avoid using the same buffer if next's data would make cumulation exceed the wrapDataSize.
                    // Only copy if there is enough space available and the capacity is large enough, and attempt to
                    // resize if the capacity is small.
                    (cumulation.isWritable(inReadableBytes) && cumulationCapacity >= wrapDataSize ||
                            cumulationCapacity < wrapDataSize &&
                                    ensureWritableSuccess(cumulation.ensureWritable(inReadableBytes, false)))) {
                cumulation.writeBytes(next);
                next.release();
                return true;
            }
            return false;
        }
    }

    private final class LazyChannelPromise extends DefaultPromise<Channel> {

        @Override
        protected EventExecutor executor() {
            if (ctx == null) {
                throw new IllegalStateException();
            }
            return ctx.executor();
        }

        @Override
        protected void checkDeadLock() {
            if (ctx == null) {
                // If ctx is null the handlerAdded(...) callback was not called, in this case the checkDeadLock()
                // method was called from another Thread then the one that is used by ctx.executor(). We need to
                // guard against this as a user can see a race if handshakeFuture().sync() is called but the
                // handlerAdded(..) method was not yet as it is called from the EventExecutor of the
                // ChannelHandlerContext. If we not guard against this super.checkDeadLock() would cause an
                // IllegalStateException when trying to call executor().
                return;
            }
            super.checkDeadLock();
        }
    }
}
