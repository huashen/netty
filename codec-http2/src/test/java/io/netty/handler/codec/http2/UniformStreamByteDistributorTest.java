/*
 * Copyright 2015 The Netty Project
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
package io.netty.handler.codec.http2;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_MIN_ALLOCATION_CHUNK;
import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests for {@link UniformStreamByteDistributor}.
 */
public class UniformStreamByteDistributorTest {
    private static final int CHUNK_SIZE = DEFAULT_MIN_ALLOCATION_CHUNK;

    private static final int STREAM_A = 1;
    private static final int STREAM_B = 3;
    private static final int STREAM_C = 5;
    private static final int STREAM_D = 7;

    private Http2Connection connection;
    private UniformStreamByteDistributor distributor;

    @Mock
    private StreamByteDistributor.Writer writer;

    @Before
    public void setup() throws Http2Exception {
        MockitoAnnotations.initMocks(this);

        connection = new DefaultHttp2Connection(false);
        distributor = new UniformStreamByteDistributor(connection);

        // Assume we always write all the allocated bytes.
        resetWriter();

        connection.local().createStream(STREAM_A, false);
        connection.local().createStream(STREAM_B, false);
        Http2Stream streamC = connection.local().createStream(STREAM_C, false);
        Http2Stream streamD = connection.local().createStream(STREAM_D, false);
        setPriority(streamC.id(), STREAM_A, DEFAULT_PRIORITY_WEIGHT, false);
        setPriority(streamD.id(), STREAM_A, DEFAULT_PRIORITY_WEIGHT, false);
    }

    private Answer<Void> writeAnswer() {
        return new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock in) throws Throwable {
                Http2Stream stream = in.getArgument(0);
                int numBytes = in.getArgument(1);
                int streamableBytes = distributor.streamableBytes0(stream) - numBytes;
                updateStream(stream.id(), streamableBytes, streamableBytes > 0);
                return null;
            }
        };
    }

    private void resetWriter() {
        reset(writer);
        doAnswer(writeAnswer()).when(writer).write(any(Http2Stream.class), anyInt());
    }

    @Test
    public void bytesUnassignedAfterProcessing() throws Http2Exception {
        updateStream(STREAM_A, 1, true);
        updateStream(STREAM_B, 2, true);
        updateStream(STREAM_C, 3, true);
        updateStream(STREAM_D, 4, true);

        assertFalse(write(10));
        verifyWrite(STREAM_A, 1);
        verifyWrite(STREAM_B, 2);
        verifyWrite(STREAM_C, 3);
        verifyWrite(STREAM_D, 4);
        verifyNoMoreInteractions(writer);

        assertFalse(write(10));
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void connectionErrorForWriterException() throws Http2Exception {
        updateStream(STREAM_A, 1, true);
        updateStream(STREAM_B, 2, true);
        updateStream(STREAM_C, 3, true);
        updateStream(STREAM_D, 4, true);

        Exception fakeException = new RuntimeException("Fake exception");
        doThrow(fakeException).when(writer).write(same(stream(STREAM_C)), eq(3));

        try {
            write(10);
            fail("Expected an exception");
        } catch (Http2Exception e) {
            assertFalse(Http2Exception.isStreamError(e));
            assertEquals(Http2Error.INTERNAL_ERROR, e.error());
            assertSame(fakeException, e.getCause());
        }

        verifyWrite(atMost(1), STREAM_A, 1);
        verifyWrite(atMost(1), STREAM_B, 2);
        verifyWrite(STREAM_C, 3);
        verifyWrite(atMost(1), STREAM_D, 4);

        doNothing().when(writer).write(same(stream(STREAM_C)), eq(3));
        write(10);
        verifyWrite(STREAM_A, 1);
        verifyWrite(STREAM_B, 2);
        verifyWrite(STREAM_C, 3);
        verifyWrite(STREAM_D, 4);
    }

    /**
     * In this test, we verify that each stream is allocated a minimum chunk size. When bytes
     * run out, the remaining streams will be next in line for the next iteration.
     */
    @Test
    public void minChunkShouldBeAllocatedPerStream() throws Http2Exception {
        // Re-assign weights.
        setPriority(STREAM_A, 0, (short) 50, false);
        setPriority(STREAM_B, 0, (short) 200, false);
        setPriority(STREAM_C, STREAM_A, (short) 100, false);
        setPriority(STREAM_D, STREAM_A, (short) 100, false);

        // Update the streams.
        updateStream(STREAM_A, CHUNK_SIZE, true);
        updateStream(STREAM_B, CHUNK_SIZE, true);
        updateStream(STREAM_C, CHUNK_SIZE, true);
        updateStream(STREAM_D, CHUNK_SIZE, true);

        // Only write 3 * chunkSize, so that we'll only write to the first 3 streams.
        int written = 3 * CHUNK_SIZE;
        assertTrue(write(written));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_A));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_B));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_C));
        verifyNoMoreInteractions(writer);

        resetWriter();

        // Now write again and verify that the last stream is written to.
        assertFalse(write(CHUNK_SIZE));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_D));
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void streamWithMoreDataShouldBeEnqueuedAfterWrite() throws Http2Exception {
        // Give the stream a bunch of data.
        updateStream(STREAM_A, 2 * CHUNK_SIZE, true);

        // Write only part of the data.
        assertTrue(write(CHUNK_SIZE));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_A));
        verifyNoMoreInteractions(writer);

        resetWriter();

        // Now write the rest of the data.
        assertFalse(write(CHUNK_SIZE));
        assertEquals(CHUNK_SIZE, captureWrite(STREAM_A));
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void emptyFrameAtHeadIsWritten() throws Http2Exception {
        updateStream(STREAM_A, 10, true);
        updateStream(STREAM_B, 0, true);
        updateStream(STREAM_C, 0, true);
        updateStream(STREAM_D, 10, true);

        assertTrue(write(10));
        verifyWrite(STREAM_A, 10);
        verifyWrite(STREAM_B, 0);
        verifyWrite(STREAM_C, 0);
        verifyNoMoreInteractions(writer);
    }

    @Test
    public void streamWindowExhaustedDoesNotWrite() throws Http2Exception {
        updateStream(STREAM_A, 0, true, false);
        updateStream(STREAM_B, 0, true);
        updateStream(STREAM_C, 0, true);
        updateStream(STREAM_D, 0, true, false);

        assertFalse(write(10));
        verifyWrite(STREAM_B, 0);
        verifyWrite(STREAM_C, 0);
        verifyNoMoreInteractions(writer);
    }

    private Http2Stream stream(int streamId) {
        return connection.stream(streamId);
    }

    private void updateStream(final int streamId, final int streamableBytes, final boolean hasFrame) {
        updateStream(streamId, streamableBytes, hasFrame, hasFrame);
    }

    private void updateStream(final int streamId, final int pendingBytes, final boolean hasFrame,
            final boolean isWriteAllowed) {
        final Http2Stream stream = stream(streamId);
        distributor.updateStreamableBytes(new StreamByteDistributor.StreamState() {
            @Override
            public Http2Stream stream() {
                return stream;
            }

            @Override
            public int pendingBytes() {
                return pendingBytes;
            }

            @Override
            public boolean hasFrame() {
                return hasFrame;
            }

            @Override
            public int windowSize() {
                return isWriteAllowed ? pendingBytes : -1;
            }
        });
    }

    private void setPriority(int streamId, int parent, int weight, boolean exclusive) {
        distributor.updateDependencyTree(streamId, parent, (short) weight, exclusive);
    }

    private boolean write(int numBytes) throws Http2Exception {
        return distributor.distribute(numBytes, writer);
    }

    private void verifyWrite(int streamId, int numBytes) {
        verify(writer).write(same(stream(streamId)), eq(numBytes));
    }

    private void verifyWrite(VerificationMode mode, int streamId, int numBytes) {
        verify(writer, mode).write(same(stream(streamId)), eq(numBytes));
    }

    private int captureWrite(int streamId) {
        ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
        verify(writer).write(same(stream(streamId)), captor.capture());
        return captor.getValue();
    }
}
