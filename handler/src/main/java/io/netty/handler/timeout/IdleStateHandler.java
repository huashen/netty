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
package io.netty.handler.timeout;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.Channel.Unsafe;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Triggers an {@link IdleStateEvent} when a {@link Channel} has not performed
 * read, write, or both operation for a while.
 *
 * <h3>Supported idle states</h3>
 * <table border="1">
 * <tr>
 * <th>Property</th><th>Meaning</th>
 * </tr>
 * <tr>
 * <td>{@code readerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#READER_IDLE}
 *     will be triggered when no read was performed for the specified period of
 *     time.  Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code writerIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#WRITER_IDLE}
 *     will be triggered when no write was performed for the specified period of
 *     time.  Specify {@code 0} to disable.</td>
 * </tr>
 * <tr>
 * <td>{@code allIdleTime}</td>
 * <td>an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE}
 *     will be triggered when neither read nor write was performed for the
 *     specified period of time.  Specify {@code 0} to disable.</td>
 * </tr>
 * </table>
 *
 * <pre>
 * // An example that sends a ping message when there is no outbound traffic
 * // for 30 seconds.  The connection is closed when there is no inbound traffic
 * // for 60 seconds.
 *
 * public class MyChannelInitializer extends {@link ChannelInitializer}&lt;{@link Channel}&gt; {
 *     {@code @Override}
 *     public void initChannel({@link Channel} channel) {
 *         channel.pipeline().addLast("idleStateHandler", new {@link IdleStateHandler}(60, 30, 0));
 *         channel.pipeline().addLast("myHandler", new MyHandler());
 *     }
 * }
 *
 * // Handler should handle the {@link IdleStateEvent} triggered by {@link IdleStateHandler}.
 * public class MyHandler extends {@link ChannelDuplexHandler} {
 *     {@code @Override}
 *     public void userEventTriggered({@link ChannelHandlerContext} ctx, {@link Object} evt) throws {@link Exception} {
 *         if (evt instanceof {@link IdleStateEvent}) {
 *             {@link IdleStateEvent} e = ({@link IdleStateEvent}) evt;
 *             if (e.state() == {@link IdleState}.READER_IDLE) {
 *                 ctx.close();
 *             } else if (e.state() == {@link IdleState}.WRITER_IDLE) {
 *                 ctx.writeAndFlush(new PingMessage());
 *             }
 *         }
 *     }
 * }
 *
 * {@link ServerBootstrap} bootstrap = ...;
 * ...
 * bootstrap.childHandler(new MyChannelInitializer());
 * ...
 * </pre>
 *
 * @see ReadTimeoutHandler
 * @see WriteTimeoutHandler
 */
public class IdleStateHandler extends ChannelDuplexHandler {
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    // Not create a new ChannelFutureListener per write operation to reduce GC pressure.
    private final ChannelFutureListener writeListener = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            lastWriteTime = ticksInNanos();
            firstWriterIdleEvent = firstAllIdleEvent = true;
        }
    };

    //是否考虑出站时较慢的情况。默认值是false（不考虑）
    private final boolean observeOutput;
    // 读事件空闲时间，0 则禁用事件
    private final long readerIdleTimeNanos;
    // 写事件空闲时间，0 则禁用事件
    private final long writerIdleTimeNanos;
    //读或写空闲时间，0 则禁用事件
    private final long allIdleTimeNanos;

    private ScheduledFuture<?> readerIdleTimeout;
    private long lastReadTime;
    private boolean firstReaderIdleEvent = true;

    private ScheduledFuture<?> writerIdleTimeout;
    private long lastWriteTime;
    private boolean firstWriterIdleEvent = true;

    private ScheduledFuture<?> allIdleTimeout;
    private boolean firstAllIdleEvent = true;

    private byte state; // 0 - none, 1 - initialized, 2 - destroyed
    private boolean reading;

    private long lastChangeCheckTimeStamp;
    private int lastMessageHashCode;
    private long lastPendingWriteBytes;

    /**
     * Creates a new instance firing {@link IdleStateEvent}s.
     *
     * @param readerIdleTimeSeconds
     *        an {@link IdleStateEvent} whose state is {@link IdleState#READER_IDLE}
     *        will be triggered when no read was performed for the specified
     *        period of time.  Specify {@code 0} to disable.
     * @param writerIdleTimeSeconds
     *        an {@link IdleStateEvent} whose state is {@link IdleState#WRITER_IDLE}
     *        will be triggered when no write was performed for the specified
     *        period of time.  Specify {@code 0} to disable.
     * @param allIdleTimeSeconds
     *        an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE}
     *        will be triggered when neither read nor write was performed for
     *        the specified period of time.  Specify {@code 0} to disable.
     */
    /**
     * 初始化3个属性
     * @param readerIdleTimeSeconds 读超时时间
     * @param writerIdleTimeSeconds 写超时时间
     * @param allIdleTimeSeconds  读写超时时间
     */
    public IdleStateHandler(
            int readerIdleTimeSeconds,
            int writerIdleTimeSeconds,
            int allIdleTimeSeconds) {

        this(readerIdleTimeSeconds, writerIdleTimeSeconds, allIdleTimeSeconds,
             TimeUnit.SECONDS);
    }

    /**
     * @see #IdleStateHandler(boolean, long, long, long, TimeUnit)
     */
    public IdleStateHandler(
            long readerIdleTime, long writerIdleTime, long allIdleTime,
            TimeUnit unit) {
        this(false, readerIdleTime, writerIdleTime, allIdleTime, unit);
    }

    /**
     * Creates a new instance firing {@link IdleStateEvent}s.
     *
     * @param observeOutput
     *        whether or not the consumption of {@code bytes} should be taken into
     *        consideration when assessing write idleness. The default is {@code false}.
     * @param readerIdleTime
     *        an {@link IdleStateEvent} whose state is {@link IdleState#READER_IDLE}
     *        will be triggered when no read was performed for the specified
     *        period of time.  Specify {@code 0} to disable.
     * @param writerIdleTime
     *        an {@link IdleStateEvent} whose state is {@link IdleState#WRITER_IDLE}
     *        will be triggered when no write was performed for the specified
     *        period of time.  Specify {@code 0} to disable.
     * @param allIdleTime
     *        an {@link IdleStateEvent} whose state is {@link IdleState#ALL_IDLE}
     *        will be triggered when neither read nor write was performed for
     *        the specified period of time.  Specify {@code 0} to disable.
     * @param unit
     *        the {@link TimeUnit} of {@code readerIdleTime},
     *        {@code writeIdleTime}, and {@code allIdleTime}
     */
    public IdleStateHandler(boolean observeOutput,
            long readerIdleTime, long writerIdleTime, long allIdleTime,
            TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        this.observeOutput = observeOutput;

        if (readerIdleTime <= 0) {
            readerIdleTimeNanos = 0;
        } else {
            readerIdleTimeNanos = Math.max(unit.toNanos(readerIdleTime), MIN_TIMEOUT_NANOS);
        }
        if (writerIdleTime <= 0) {
            writerIdleTimeNanos = 0;
        } else {
            writerIdleTimeNanos = Math.max(unit.toNanos(writerIdleTime), MIN_TIMEOUT_NANOS);
        }
        if (allIdleTime <= 0) {
            allIdleTimeNanos = 0;
        } else {
            allIdleTimeNanos = Math.max(unit.toNanos(allIdleTime), MIN_TIMEOUT_NANOS);
        }
    }

    /**
     * Return the readerIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getReaderIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(readerIdleTimeNanos);
    }

    /**
     * Return the writerIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getWriterIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(writerIdleTimeNanos);
    }

    /**
     * Return the allIdleTime that was given when instance this class in milliseconds.
     *
     */
    public long getAllIdleTimeInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(allIdleTimeNanos);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        //当该 handler 被添加到 pipeline 中时，则调用 initialize 方法
        if (ctx.channel().isActive() && ctx.channel().isRegistered()) {
            // channelActive() event has been fired already, which means this.channelActive() will
            // not be invoked. We have to initialize here instead.
            initialize(ctx);
        } else {
            // channelActive() event has not been fired yet.  this.channelActive() will be invoked
            // and initialization will occur there.
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        destroy();
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Initialize early if channel is active already.
        if (ctx.channel().isActive()) {
            initialize(ctx);
        }
        super.channelRegistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // This method will be invoked only if this handler was added
        // before channelActive() event is fired.  If a user adds this handler
        // after the channelActive() event, initialize() will be called by beforeAdd().
        initialize(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        destroy();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {
            reading = true;
            firstReaderIdleEvent = firstAllIdleEvent = true;
        }
        ctx.fireChannelRead(msg);
    }

    /**
     * 在读取网络数据的时候，会先调用 channelRead 方法，
     * 等缓冲区中的数据读取完成之后，暂时没有数据可读了
     * 会再调用 channelReadComplete 方法
     * reading = true表示读取中 调用完channelReadComplete方法后会把reading置为false
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if ((readerIdleTimeNanos > 0 || allIdleTimeNanos > 0) && reading) {
            lastReadTime = ticksInNanos();
            reading = false;
        }
        ctx.fireChannelReadComplete();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Allow writing with void promise if handler is only configured for read timeout events.
        if (writerIdleTimeNanos > 0 || allIdleTimeNanos > 0) {
            ctx.write(msg, promise.unvoid()).addListener(writeListener);
        } else {
            ctx.write(msg, promise);
        }
    }

    /**
     * 初始化
     * 将读 / 写 / 读写超时任务加入到定时任务队列中
     * @param ctx
     */
    private void initialize(ChannelHandlerContext ctx) {
        // Avoid the case where destroy() is called before scheduling timeouts.
        // See: https://github.com/netty/netty/issues/143
        switch (state) {
        case 1:
        case 2:
            return;
        }

        //将 state 状态设置为 1，防止重复初始化
        state = 1;
        //初始化 “监控出站数据属性”
        initOutputChanged(ctx);

        lastReadTime = lastWriteTime = ticksInNanos();
        if (readerIdleTimeNanos > 0) {
            // 这里的 schedule 方法会调用 eventLoop 的 schedule 方法，将定时任务添加进队列中
            readerIdleTimeout = schedule(ctx, new ReaderIdleTimeoutTask(ctx),
                    readerIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
        if (writerIdleTimeNanos > 0) {
            writerIdleTimeout = schedule(ctx, new WriterIdleTimeoutTask(ctx),
                    writerIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
        if (allIdleTimeNanos > 0) {
            allIdleTimeout = schedule(ctx, new AllIdleTimeoutTask(ctx),
                    allIdleTimeNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * This method is visible for testing!
     */
    long ticksInNanos() {
        return System.nanoTime();
    }

    /**
     * This method is visible for testing!
     */
    ScheduledFuture<?> schedule(ChannelHandlerContext ctx, Runnable task, long delay, TimeUnit unit) {
        return ctx.executor().schedule(task, delay, unit);
    }

    private void destroy() {
        state = 2;

        if (readerIdleTimeout != null) {
            readerIdleTimeout.cancel(false);
            readerIdleTimeout = null;
        }
        if (writerIdleTimeout != null) {
            writerIdleTimeout.cancel(false);
            writerIdleTimeout = null;
        }
        if (allIdleTimeout != null) {
            allIdleTimeout.cancel(false);
            allIdleTimeout = null;
        }
    }

    /**
     * Is called when an {@link IdleStateEvent} should be fired. This implementation calls
     * {@link ChannelHandlerContext#fireUserEventTriggered(Object)}.
     */
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * Returns a {@link IdleStateEvent}.
     */
    protected IdleStateEvent newIdleStateEvent(IdleState state, boolean first) {
        switch (state) {
            case ALL_IDLE:
                return first ? IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT : IdleStateEvent.ALL_IDLE_STATE_EVENT;
            case READER_IDLE:
                return first ? IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT : IdleStateEvent.READER_IDLE_STATE_EVENT;
            case WRITER_IDLE:
                return first ? IdleStateEvent.FIRST_WRITER_IDLE_STATE_EVENT : IdleStateEvent.WRITER_IDLE_STATE_EVENT;
            default:
                throw new IllegalArgumentException("Unhandled: state=" + state + ", first=" + first);
        }
    }

    /**
     * @see #hasOutputChanged(ChannelHandlerContext, boolean)
     */
    private void initOutputChanged(ChannelHandlerContext ctx) {
        if (observeOutput) {
            Channel channel = ctx.channel();
            Unsafe unsafe = channel.unsafe();
            ChannelOutboundBuffer buf = unsafe.outboundBuffer();

            //记录了出站缓冲区相关的数据，buf 对象的 hash 码，和 buf 的剩余缓冲字节数
            if (buf != null) {
                lastMessageHashCode = System.identityHashCode(buf.current());
                lastPendingWriteBytes = buf.totalPendingWriteBytes();
            }
        }
    }

    /**
     * Returns {@code true} if and only if the {@link IdleStateHandler} was constructed
     * with {@link #observeOutput} enabled and there has been an observed change in the
     * {@link ChannelOutboundBuffer} between two consecutive calls of this method.
     *
     * https://github.com/netty/netty/issues/6150
     */
    private boolean hasOutputChanged(ChannelHandlerContext ctx, boolean first) {
        if (observeOutput) {

            // We can take this shortcut if the ChannelPromises that got passed into write()
            // appear to complete. It indicates "change" on message level and we simply assume
            // that there's change happening on byte level. If the user doesn't observe channel
            // writability events then they'll eventually OOME and there's clearly a different
            // problem and idleness is least of their concerns.
            // 如果最后一次写的时间和上一次记录的时间不一样，说明写操作进行过了，则更新此值
            if (lastChangeCheckTimeStamp != lastWriteTime) {
                lastChangeCheckTimeStamp = lastWriteTime;

                // But this applies only if it's the non-first call.
                // 但如果，在这个方法的调用间隙修改的，就仍然不触发事件
                if (!first) {
                    return true;
                }
            }

            Channel channel = ctx.channel();
            Unsafe unsafe = channel.unsafe();
            ChannelOutboundBuffer buf = unsafe.outboundBuffer();

            // 如果出站区有数据
            if (buf != null) {
                // 拿到出站缓冲区的 对象 hashcode
                int messageHashCode = System.identityHashCode(buf.current());
                // 拿到这个 缓冲区的 所有字节
                long pendingWriteBytes = buf.totalPendingWriteBytes();

                // 如果和之前的不相等，或者字节数不同，说明，输出有变化，将 "最后一个缓冲区引用" 和 “剩余字节数” 刷新
                if (messageHashCode != lastMessageHashCode || pendingWriteBytes != lastPendingWriteBytes) {
                    lastMessageHashCode = messageHashCode;
                    lastPendingWriteBytes = pendingWriteBytes;

                    // 如果写操作没有进行过，则任务写的慢，不触发空闲事件
                    if (!first) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private abstract static class AbstractIdleTask implements Runnable {

        private final ChannelHandlerContext ctx;

        AbstractIdleTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (!ctx.channel().isOpen()) {
                return;
            }

            run(ctx);
        }

        protected abstract void run(ChannelHandlerContext ctx);
    }

    private final class ReaderIdleTimeoutTask extends AbstractIdleTask {

        ReaderIdleTimeoutTask(ChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        protected void run(ChannelHandlerContext ctx) {
            long nextDelay = readerIdleTimeNanos;
            if (!reading) {
                nextDelay -= ticksInNanos() - lastReadTime;
            }

            if (nextDelay <= 0) {
                // Reader is idle - set a new timeout and notify the callback.
                //用于取消任务 promise
                readerIdleTimeout = schedule(ctx, this, readerIdleTimeNanos, TimeUnit.NANOSECONDS);

                boolean first = firstReaderIdleEvent;
                firstReaderIdleEvent = false;

                try {
                    // 再次提交任务 读取超时,会向下传递一个读空闲事件
                    IdleStateEvent event = newIdleStateEvent(IdleState.READER_IDLE, first);
                    // 触发用户 handler use
                    /**
                     * 创建一个 IdleStateEvent 类型的写事件对象，
                     * 将此对象传递给用户的 UserEventTriggered 方法。完成触发事件的操作
                     */
                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Read occurred before the timeout - set a new timeout with shorter delay.
                // 安全范围
                readerIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }

    private final class WriterIdleTimeoutTask extends AbstractIdleTask {

        WriterIdleTimeoutTask(ChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        protected void run(ChannelHandlerContext ctx) {

            long lastWriteTime = IdleStateHandler.this.lastWriteTime;
            long nextDelay = writerIdleTimeNanos - (ticksInNanos() - lastWriteTime);
            if (nextDelay <= 0) {
                // Writer is idle - set a new timeout and notify the callback.
                writerIdleTimeout = schedule(ctx, this, writerIdleTimeNanos, TimeUnit.NANOSECONDS);

                boolean first = firstWriterIdleEvent;
                firstWriterIdleEvent = false;

                try {
                    if (hasOutputChanged(ctx, first)) {
                        return;
                    }

                    IdleStateEvent event = newIdleStateEvent(IdleState.WRITER_IDLE, first);
                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Write occurred before the timeout - set a new timeout with shorter delay.
                writerIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }

    private final class AllIdleTimeoutTask extends AbstractIdleTask {

        AllIdleTimeoutTask(ChannelHandlerContext ctx) {
            super(ctx);
        }

        @Override
        protected void run(ChannelHandlerContext ctx) {

            long nextDelay = allIdleTimeNanos;
            if (!reading) {
                // 当前时间减去 最后一次写或读 的时间 ，若大于0，说明超时了
                nextDelay -= ticksInNanos() - Math.max(lastReadTime, lastWriteTime);
            }
            if (nextDelay <= 0) {
                // Both reader and writer are idle - set a new timeout and
                // notify the callback.
                allIdleTimeout = schedule(ctx, this, allIdleTimeNanos, TimeUnit.NANOSECONDS);

                boolean first = firstAllIdleEvent;
                firstAllIdleEvent = false;

                try {
                    if (hasOutputChanged(ctx, first)) {
                        return;
                    }

                    IdleStateEvent event = newIdleStateEvent(IdleState.ALL_IDLE, first);
                    channelIdle(ctx, event);
                } catch (Throwable t) {
                    ctx.fireExceptionCaught(t);
                }
            } else {
                // Either read or write occurred before the timeout - set a new
                // timeout with shorter delay.
                allIdleTimeout = schedule(ctx, this, nextDelay, TimeUnit.NANOSECONDS);
            }
        }
    }
}
