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

package io.netty.handler.codec.http2;

import io.netty.handler.codec.http2.Http2Stream.State;
import io.netty.util.internal.UnstableApi;

/**
 * A single stream within a HTTP/2 connection. To be used with the {@link Http2FrameCodec}.
 */
@UnstableApi
public interface Http2FrameStream {

    /**
     * The stream with identifier 0, representing the HTTP/2 connection.
     */
    Http2FrameStream CONNECTION_STREAM = new Http2FrameStream() {

        @Override
        public int id() {
            return 0;
        }

        @Override
        public State state() {
            return State.IDLE;
        }
    };

    /**
     * Returns the stream identifier.
     *
     * <p>Use {@link Http2CodecUtil#isStreamIdValid(int)} to check if the stream has already been assigned an
     * identifier.
     */
    int id();

    /**
     * Returns the state of this stream.
     */
    State state();
}
