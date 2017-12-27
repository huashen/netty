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
package io.netty.channel.kqueue;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.kqueue.AbstractKQueueChannel.AbstractKQueueUnsafe;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.IovArray;
import io.netty.util.IntSupplier;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.channel.kqueue.KQueueEventArray.deleteGlobalRefs;
import static java.lang.Math.min;

/**
 * {@link EventLoop} which uses kqueue under the covers. Only works on BSD!
 */
final class KQueueEventLoop extends SingleThreadEventLoop {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(KQueueEventLoop.class);
    private static final AtomicIntegerFieldUpdater<KQueueEventLoop> WAKEN_UP_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(KQueueEventLoop.class, "wakenUp");
    private static final int KQUEUE_WAKE_UP_IDENT = 0;

    static {
        // Ensure JNI is initialized by the time this class is loaded by this time!
        // We use unix-common methods in this class which are backed by JNI methods.
        KQueue.ensureAvailability();
    }

    private final NativeLongArray jniChannelPointers;
    private final boolean allowGrowing;
    private final FileDescriptor kqueueFd;
    private final KQueueEventArray changeList;
    private final KQueueEventArray eventList;
    private final SelectStrategy selectStrategy;
    private final IovArray iovArray = new IovArray();
    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            return kqueueWaitNow();
        }
    };
    private final Callable<Integer> pendingTasksCallable = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
            return KQueueEventLoop.super.pendingTasks();
        }
    };

    private volatile int wakenUp;
    private volatile int ioRatio = 50;

    KQueueEventLoop(EventLoopGroup parent, Executor executor, int maxEvents,
                    SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler) {
        super(parent, executor, false, DEFAULT_MAX_PENDING_TASKS, rejectedExecutionHandler);
        selectStrategy = ObjectUtil.checkNotNull(strategy, "strategy");
        this.kqueueFd = Native.newKQueue();
        if (maxEvents == 0) {
            allowGrowing = true;
            maxEvents = 4096;
        } else {
            allowGrowing = false;
        }
        changeList = new KQueueEventArray(maxEvents);
        eventList = new KQueueEventArray(maxEvents);
        jniChannelPointers = new NativeLongArray(4096);
        int result = Native.keventAddUserEvent(kqueueFd.intValue(), KQUEUE_WAKE_UP_IDENT);
        if (result < 0) {
            cleanup();
            throw new IllegalStateException("kevent failed to add user event with errno: " + (-result));
        }
    }

    void evSet(AbstractKQueueChannel ch, short filter, short flags, int fflags) {
        changeList.evSet(ch, filter, flags, fflags);
    }

    void remove(AbstractKQueueChannel ch) throws IOException {
        assert inEventLoop();
        if (ch.jniSelfPtr == 0) {
            return;
        }

        jniChannelPointers.add(ch.jniSelfPtr);
        ch.jniSelfPtr = 0;
    }

    /**
     * Return a cleared {@link IovArray} that can be used for writes in this {@link EventLoop}.
     */
    IovArray cleanArray() {
        iovArray.clear();
        return iovArray;
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && WAKEN_UP_UPDATER.compareAndSet(this, 0, 1)) {
            wakeup();
        }
    }

    private void wakeup() {
        Native.keventTriggerUserEvent(kqueueFd.intValue(), KQUEUE_WAKE_UP_IDENT);
        // Note that the result may return an error (e.g. errno = EBADF after the event loop has been shutdown).
        // So it is not very practical to assert the return value is always >= 0.
    }

    private int kqueueWait(boolean oldWakeup) throws IOException {
        // If a task was submitted when wakenUp value was 1, the task didn't get a chance to produce wakeup event.
        // So we need to check task queue again before calling kqueueWait. If we don't, the task might be pended
        // until kqueueWait was timed out. It might be pended until idle timeout if IdleStateHandler existed
        // in pipeline.
        if (oldWakeup && hasTasks()) {
            return kqueueWaitNow();
        }

        long totalDelay = delayNanos(System.nanoTime());
        int delaySeconds = (int) min(totalDelay / 1000000000L, Integer.MAX_VALUE);
        return kqueueWait(delaySeconds, (int) min(totalDelay - delaySeconds * 1000000000L, Integer.MAX_VALUE));
    }

    private int kqueueWaitNow() throws IOException {
        return kqueueWait(0, 0);
    }

    private int kqueueWait(int timeoutSec, int timeoutNs) throws IOException {
        deleteJniChannelPointers();
        int numEvents = Native.keventWait(kqueueFd.intValue(), changeList, eventList, timeoutSec, timeoutNs);
        changeList.clear();
        return numEvents;
    }

    private void deleteJniChannelPointers() {
        if (!jniChannelPointers.isEmpty()) {
            deleteGlobalRefs(jniChannelPointers.memoryAddress(), jniChannelPointers.memoryAddressEnd());
            jniChannelPointers.clear();
        }
    }

    private void processReady(int ready) {
        for (int i = 0; i < ready; ++i) {
            final short filter = eventList.filter(i);
            final short flags = eventList.flags(i);
            if (filter == Native.EVFILT_USER || (flags & Native.EV_ERROR) != 0) {
                // EV_ERROR is returned if the FD is closed synchronously (which removes from kqueue) and then
                // we later attempt to delete the filters from kqueue.
                assert filter != Native.EVFILT_USER ||
                        (filter == Native.EVFILT_USER && eventList.fd(i) == KQUEUE_WAKE_UP_IDENT);
                continue;
            }

            AbstractKQueueChannel channel = eventList.channel(i);
            if (channel == null) {
                // This may happen if the channel has already been closed, and it will be removed from kqueue anyways.
                // We also handle EV_ERROR above to skip this even early if it is a result of a referencing a closed and
                // thus removed from kqueue FD.
                logger.warn("events[{}]=[{}, {}] had no channel!", i, eventList.fd(i), filter);
                continue;
            }

            AbstractKQueueUnsafe unsafe = (AbstractKQueueUnsafe) channel.unsafe();
            // First check for EPOLLOUT as we may need to fail the connect ChannelPromise before try
            // to read from the file descriptor.
            if (filter == Native.EVFILT_WRITE) {
                unsafe.writeReady();
            } else if (filter == Native.EVFILT_READ) {
                // Check READ before EOF to ensure all data is read before shutting down the input.
                unsafe.readReady(eventList.data(i));
            } else if (filter == Native.EVFILT_SOCK && (eventList.fflags(i) & Native.NOTE_RDHUP) != 0) {
                unsafe.readEOF();
            }

            // Check if EV_EOF was set, this will notify us for connection-reset in which case
            // we may close the channel directly or try to read more data depending on the state of the
            // Channel and also depending on the AbstractKQueueChannel subtype.
            if ((flags & Native.EV_EOF) != 0) {
                unsafe.readEOF();
            }
        }
    }

    @Override
    protected void run() {
        for (;;) {
            try {
                int strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                switch (strategy) {
                    case SelectStrategy.CONTINUE:
                        continue;
                    case SelectStrategy.SELECT:
                        strategy = kqueueWait(WAKEN_UP_UPDATER.getAndSet(this, 0) == 1);

                        // 'wakenUp.compareAndSet(false, true)' is always evaluated
                        // before calling 'selector.wakeup()' to reduce the wake-up
                        // overhead. (Selector.wakeup() is an expensive operation.)
                        //
                        // However, there is a race condition in this approach.
                        // The race condition is triggered when 'wakenUp' is set to
                        // true too early.
                        //
                        // 'wakenUp' is set to true too early if:
                        // 1) Selector is waken up between 'wakenUp.set(false)' and
                        //    'selector.select(...)'. (BAD)
                        // 2) Selector is waken up between 'selector.select(...)' and
                        //    'if (wakenUp.get()) { ... }'. (OK)
                        //
                        // In the first case, 'wakenUp' is set to true and the
                        // following 'selector.select(...)' will wake up immediately.
                        // Until 'wakenUp' is set to false again in the next round,
                        // 'wakenUp.compareAndSet(false, true)' will fail, and therefore
                        // any attempt to wake up the Selector will fail, too, causing
                        // the following 'selector.select(...)' call to block
                        // unnecessarily.
                        //
                        // To fix this problem, we wake up the selector again if wakenUp
                        // is true immediately after selector.select(...).
                        // It is inefficient in that it wakes up the selector for both
                        // the first case (BAD - wake-up required) and the second case
                        // (OK - no wake-up required).

                        if (wakenUp == 1) {
                            wakeup();
                        }
                        // fallthrough
                    default:
                }

                final int ioRatio = this.ioRatio;
                if (ioRatio == 100) {
                    try {
                        if (strategy > 0) {
                            processReady(strategy);
                        }
                    } finally {
                        runAllTasks();
                    }
                } else {
                    final long ioStartTime = System.nanoTime();

                    try {
                        if (strategy > 0) {
                            processReady(strategy);
                        }
                    } finally {
                        final long ioTime = System.nanoTime() - ioStartTime;
                        runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
                    }
                }
                if (allowGrowing && strategy == eventList.capacity()) {
                    //increase the size of the array as we needed the whole space for the events
                    eventList.realloc(false);
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
            // Always handle shutdown even if the loop processing threw an exception.
            try {
                if (isShuttingDown()) {
                    closeAll();
                    if (confirmShutdown()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                                                    : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    @Override
    public int pendingTasks() {
        // As we use a MpscQueue we need to ensure pendingTasks() is only executed from within the EventLoop as
        // otherwise we may see unexpected behavior (as size() is only allowed to be called by a single consumer).
        // See https://github.com/netty/netty/issues/5297
        return inEventLoop() ? super.pendingTasks() : submit(pendingTasksCallable).syncUninterruptibly().getNow();
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }

    @Override
    protected void cleanup() {
        try {
            try {
                kqueueFd.close();
            } catch (IOException e) {
                logger.warn("Failed to close the kqueue fd.", e);
            }
        } finally {
            // Cleanup all native memory!

            // The JNI channel pointers should already be deleted because we should wait on kevent before this method,
            // but lets just be sure we cleanup native memory.
            deleteJniChannelPointers();
            jniChannelPointers.free();

            changeList.free();
            eventList.free();
        }
    }

    private void closeAll() {
        try {
            kqueueWaitNow();
        } catch (IOException e) {
            // ignore on close
        }
    }

    private static void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }
}
