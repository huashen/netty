/*
 * Copyright 2017 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.ArrayDeque;

import static io.netty.util.internal.ObjectUtil.checkNotNull;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

@UnstableApi
public abstract class AbstractCoalescingBufferQueue {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AbstractCoalescingBufferQueue.class);
    private final ArrayDeque<Object> bufAndListenerPairs;
    private int readableBytes;

    public AbstractCoalescingBufferQueue(int initSize) {
        bufAndListenerPairs = new ArrayDeque<Object>(initSize);
    }

    /**
     * Add a buffer to the front of the queue.
     */
    public final void addFirst(ByteBuf buf) {
        incrementReadableBytes(buf.readableBytes());
        // Listener would be added here, but since it is null there is no need. The assumption is there is already a
        // listener at the front of the queue, or there is a buffer at the front of the queue, which was spliced from
        // buf via remove().
        bufAndListenerPairs.addFirst(buf);
    }

    /**
     * Add a buffer to the end of the queue.
     */
    public final void add(ByteBuf buf) {
        add(buf, (ChannelFutureListener) null);
    }

    /**
     * Add a buffer to the end of the queue and associate a promise with it that should be completed when
     * all the buffers bytes have been consumed from the queue and written.
     * @param buf to add to the tail of the queue
     * @param promise to complete when all the bytes have been consumed and written, can be void.
     */
    public final void add(ByteBuf buf, ChannelPromise promise) {
        // buffers are added before promises so that we naturally 'consume' the entire buffer during removal
        // before we complete it's promise.
        add(buf, promise.isVoid() ? null : (ChannelFutureListener) new DelegatingChannelPromiseNotifier(promise));
    }

    /**
     * Add a buffer to the end of the queue and associate a listener with it that should be completed when
     * all the buffers  bytes have been consumed from the queue and written.
     * @param buf to add to the tail of the queue
     * @param listener to notify when all the bytes have been consumed and written, can be {@code null}.
     */
    public final void add(ByteBuf buf, ChannelFutureListener listener) {
        // buffers are added before promises so that we naturally 'consume' the entire buffer during removal
        // before we complete it's promise.
        incrementReadableBytes(buf.readableBytes());
        bufAndListenerPairs.add(buf);
        if (listener != null) {
            bufAndListenerPairs.add(listener);
        }
    }

    /**
     * Remove the first {@link ByteBuf} from the queue.
     * @param aggregatePromise used to aggregate the promises and listeners for the returned buffer.
     * @return the first {@link ByteBuf} from the queue.
     */
    public final ByteBuf removeFirst(ChannelPromise aggregatePromise) {
        Object entry = bufAndListenerPairs.poll();
        if (entry == null) {
            return null;
        }
        assert entry instanceof ByteBuf;
        ByteBuf result = (ByteBuf) entry;

        readableBytes -= result.readableBytes();
        assert readableBytes >= 0;

        entry = bufAndListenerPairs.peek();
        if (entry instanceof ChannelFutureListener) {
            aggregatePromise.addListener((ChannelFutureListener) entry);
            bufAndListenerPairs.poll();
        }
        return result;
    }

    /**
     * Remove a {@link ByteBuf} from the queue with the specified number of bytes. Any added buffer who's bytes are
     * fully consumed during removal will have it's promise completed when the passed aggregate {@link ChannelPromise}
     * completes.
     *
     * @param alloc The allocator used if a new {@link ByteBuf} is generated during the aggregation process.
     * @param bytes the maximum number of readable bytes in the returned {@link ByteBuf}, if {@code bytes} is greater
     *              than {@link #readableBytes} then a buffer of length {@link #readableBytes} is returned.
     * @param aggregatePromise used to aggregate the promises and listeners for the constituent buffers.
     * @return a {@link ByteBuf} composed of the enqueued buffers.
     */
    public final ByteBuf remove(ByteBufAllocator alloc, int bytes, ChannelPromise aggregatePromise) {
        checkPositiveOrZero(bytes, "bytes");
        checkNotNull(aggregatePromise, "aggregatePromise");

        // Use isEmpty rather than readableBytes==0 as we may have a promise associated with an empty buffer.
        if (bufAndListenerPairs.isEmpty()) {
            return removeEmptyValue();
        }
        bytes = Math.min(bytes, readableBytes);

        ByteBuf toReturn = null;
        int originalBytes = bytes;
        for (;;) {
            Object entry = bufAndListenerPairs.poll();
            if (entry == null) {
                break;
            }
            if (entry instanceof ChannelFutureListener) {
                aggregatePromise.addListener((ChannelFutureListener) entry);
                continue;
            }
            ByteBuf entryBuffer = (ByteBuf) entry;
            if (entryBuffer.readableBytes() > bytes) {
                // Add the buffer back to the queue as we can't consume all of it.
                bufAndListenerPairs.addFirst(entryBuffer);
                if (bytes > 0) {
                    // Take a slice of what we can consume and retain it.
                    ByteBuf next = entryBuffer.readRetainedSlice(bytes);
                    toReturn = toReturn == null ? composeFirst(alloc, next) : compose(alloc, toReturn, next);
                    bytes = 0;
                }
                break;
            } else {
                bytes -= entryBuffer.readableBytes();
                toReturn = toReturn == null ? composeFirst(alloc, entryBuffer) : compose(alloc, toReturn, entryBuffer);
            }
        }
        readableBytes -= originalBytes - bytes;
        assert readableBytes >= 0;
        return toReturn;
    }

    /**
     * The number of readable bytes.
     */
    public final int readableBytes() {
        return readableBytes;
    }

    /**
     * Are there pending buffers in the queue.
     */
    public final boolean isEmpty() {
        return bufAndListenerPairs.isEmpty();
    }

    /**
     *  Release all buffers in the queue and complete all listeners and promises.
     */
    public final void releaseAndFailAll(ChannelOutboundInvoker invoker, Throwable cause) {
        releaseAndCompleteAll(invoker.newFailedFuture(cause));
    }

    /**
     * Copy all pending entries in this queue into the destination queue.
     * @param dest to copy pending buffers to.
     */
    public final void copyTo(AbstractCoalescingBufferQueue dest) {
        dest.bufAndListenerPairs.addAll(bufAndListenerPairs);
        dest.readableBytes += readableBytes;
    }

    /**
     * Writes all remaining elements in this queue.
     * @param ctx The context to write all elements to.
     */
    public final void writeAndRemoveAll(ChannelHandlerContext ctx) {
        readableBytes = 0;
        Throwable pending = null;
        ByteBuf previousBuf = null;
        for (;;) {
            Object entry = bufAndListenerPairs.poll();
            try {
                if (entry == null) {
                    if (previousBuf != null) {
                        ctx.write(previousBuf, ctx.voidPromise());
                    }
                    break;
                }

                if (entry instanceof ByteBuf) {
                    if (previousBuf != null) {
                        ctx.write(previousBuf, ctx.voidPromise());
                    }
                    previousBuf = (ByteBuf) entry;
                } else if (entry instanceof ChannelPromise) {
                    ctx.write(previousBuf, (ChannelPromise) entry);
                    previousBuf = null;
                } else {
                    ctx.write(previousBuf).addListener((ChannelFutureListener) entry);
                    previousBuf = null;
                }
            } catch (Throwable t) {
                if (pending == null) {
                    pending = t;
                } else {
                    logger.info("Throwable being suppressed because Throwable {} is already pending", pending, t);
                }
            }
        }
        if (pending != null) {
            throw new IllegalStateException(pending);
        }
    }

    /**
     * Calculate the result of {@code current + next}.
     */
    protected abstract ByteBuf compose(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf next);

    /**
     * Calculate the first {@link ByteBuf} which will be used in subsequent calls to
     * {@link #compose(ByteBufAllocator, ByteBuf, ByteBuf)}.
     */
    protected ByteBuf composeFirst(ByteBufAllocator allocator, ByteBuf first) {
        return first;
    }

    /**
     * The value to return when {@link #remove(ByteBufAllocator, int, ChannelPromise)} is called but the queue is empty.
     * @return the {@link ByteBuf} which represents an empty queue.
     */
    protected abstract ByteBuf removeEmptyValue();

    /**
     * Get the number of elements in this queue added via one of the {@link #add(ByteBuf)} methods.
     * @return the number of elements in this queue.
     */
    protected final int size() {
        return bufAndListenerPairs.size();
    }

    private void releaseAndCompleteAll(ChannelFuture future) {
        readableBytes = 0;
        Throwable pending = null;
        for (;;) {
            Object entry = bufAndListenerPairs.poll();
            if (entry == null) {
                break;
            }
            try {
                if (entry instanceof ByteBuf) {
                    ReferenceCountUtil.safeRelease(entry);
                } else {
                    ((ChannelFutureListener) entry).operationComplete(future);
                }
            } catch (Throwable t) {
                if (pending == null) {
                    pending = t;
                } else {
                    logger.info("Throwable being suppressed because Throwable {} is already pending", pending, t);
                }
            }
        }
        if (pending != null) {
            throw new IllegalStateException(pending);
        }
    }

    private void incrementReadableBytes(int increment) {
        int nextReadableBytes = readableBytes + increment;
        if (nextReadableBytes < readableBytes) {
            throw new IllegalStateException("buffer queue length overflow: " + readableBytes + " + " + increment);
        }
        readableBytes = nextReadableBytes;
    }
}
