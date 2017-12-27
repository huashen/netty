/*
 * Copyright 2015 The Netty Project
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
package io.netty.channel.unix;

import io.netty.channel.ChannelException;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static io.netty.channel.unix.Errors.ERRNO_EAGAIN_NEGATIVE;
import static io.netty.channel.unix.Errors.ERROR_ECONNREFUSED_NEGATIVE;
import static io.netty.channel.unix.Errors.ERRNO_EINPROGRESS_NEGATIVE;
import static io.netty.channel.unix.Errors.ERRNO_EWOULDBLOCK_NEGATIVE;
import static io.netty.channel.unix.Errors.ioResult;
import static io.netty.channel.unix.Errors.newIOException;
import static io.netty.channel.unix.Errors.throwConnectException;
import static io.netty.channel.unix.LimitsStaticallyReferencedJniMethods.udsSunPathSize;
import static io.netty.channel.unix.NativeInetAddress.address;
import static io.netty.channel.unix.NativeInetAddress.ipv4MappedIpv6Address;
import static io.netty.util.internal.ThrowableUtil.unknownStackTrace;

/**
 * Provides a JNI bridge to native socket operations.
 * <strong>Internal usage only!</strong>
 */
public class Socket extends FileDescriptor {
    private static final ClosedChannelException SHUTDOWN_CLOSED_CHANNEL_EXCEPTION = unknownStackTrace(
            new ClosedChannelException(), Socket.class, "shutdown(..)");
    private static final ClosedChannelException SEND_TO_CLOSED_CHANNEL_EXCEPTION = unknownStackTrace(
            new ClosedChannelException(), Socket.class, "sendTo(..)");
    private static final ClosedChannelException SEND_TO_ADDRESS_CLOSED_CHANNEL_EXCEPTION =
            unknownStackTrace(new ClosedChannelException(), Socket.class, "sendToAddress(..)");
    private static final ClosedChannelException SEND_TO_ADDRESSES_CLOSED_CHANNEL_EXCEPTION =
            unknownStackTrace(new ClosedChannelException(), Socket.class, "sendToAddresses(..)");
    private static final Errors.NativeIoException SEND_TO_CONNECTION_RESET_EXCEPTION = unknownStackTrace(
            Errors.newConnectionResetException("syscall:sendto", Errors.ERRNO_EPIPE_NEGATIVE),
            Socket.class, "sendTo(..)");
    private static final Errors.NativeIoException SEND_TO_ADDRESS_CONNECTION_RESET_EXCEPTION =
            unknownStackTrace(Errors.newConnectionResetException("syscall:sendto",
                    Errors.ERRNO_EPIPE_NEGATIVE), Socket.class, "sendToAddress");
    private static final Errors.NativeIoException CONNECTION_RESET_EXCEPTION_SENDMSG = unknownStackTrace(
            Errors.newConnectionResetException("syscall:sendmsg",
            Errors.ERRNO_EPIPE_NEGATIVE), Socket.class, "sendToAddresses(..)");
    private static final Errors.NativeIoException CONNECTION_RESET_SHUTDOWN_EXCEPTION =
            unknownStackTrace(Errors.newConnectionResetException("syscall:shutdown",
                    Errors.ERRNO_ECONNRESET_NEGATIVE), Socket.class, "shutdown");
    private static final Errors.NativeConnectException FINISH_CONNECT_REFUSED_EXCEPTION =
            unknownStackTrace(new Errors.NativeConnectException("syscall:getsockopt",
                    Errors.ERROR_ECONNREFUSED_NEGATIVE), Socket.class, "finishConnect(..)");
    private static final Errors.NativeConnectException CONNECT_REFUSED_EXCEPTION =
            unknownStackTrace(new Errors.NativeConnectException("syscall:connect",
                    Errors.ERROR_ECONNREFUSED_NEGATIVE), Socket.class, "connect(..)");

    public static final int UDS_SUN_PATH_SIZE = udsSunPathSize();

    public Socket(int fd) {
        super(fd);
    }

    public final void shutdown() throws IOException {
        shutdown(true, true);
    }

    public final void shutdown(boolean read, boolean write) throws IOException {
        for (;;) {
            // We need to only shutdown what has not been shutdown yet, and if there is no change we should not
            // shutdown anything. This is because if the underlying FD is reused and we still have an object which
            // represents the previous incarnation of the FD we need to be sure we don't inadvertently shutdown the
            // "new" FD without explicitly having a change.
            final int oldState = this.state;
            if (isClosed(oldState)) {
                throw new ClosedChannelException();
            }
            int newState = oldState;
            if (read && !isInputShutdown(newState)) {
                newState = inputShutdown(newState);
            }
            if (write && !isOutputShutdown(newState)) {
                newState = outputShutdown(newState);
            }

            // If there is no change in state, then we should not take any action.
            if (newState == oldState) {
                return;
            }
            if (casState(oldState, newState)) {
                break;
            }
        }
        int res = shutdown(fd, read, write);
        if (res < 0) {
            ioResult("shutdown", res, CONNECTION_RESET_SHUTDOWN_EXCEPTION, SHUTDOWN_CLOSED_CHANNEL_EXCEPTION);
        }
    }

    public final boolean isShutdown() {
        int state = this.state;
        return isInputShutdown(state) && isOutputShutdown(state);
    }

    public final boolean isInputShutdown() {
        return isInputShutdown(state);
    }

    public final boolean isOutputShutdown() {
        return isOutputShutdown(state);
    }

    public final int sendTo(ByteBuffer buf, int pos, int limit, InetAddress addr, int port) throws IOException {
        // just duplicate the toNativeInetAddress code here to minimize object creation as this method is expected
        // to be called frequently
        byte[] address;
        int scopeId;
        if (addr instanceof Inet6Address) {
            address = addr.getAddress();
            scopeId = ((Inet6Address) addr).getScopeId();
        } else {
            // convert to ipv4 mapped ipv6 address;
            scopeId = 0;
            address = ipv4MappedIpv6Address(addr.getAddress());
        }
        int res = sendTo(fd, buf, pos, limit, address, scopeId, port);
        if (res >= 0) {
            return res;
        }
        if (res == ERROR_ECONNREFUSED_NEGATIVE) {
            throw new PortUnreachableException("sendTo failed");
        }
        return ioResult("sendTo", res, SEND_TO_CONNECTION_RESET_EXCEPTION, SEND_TO_CLOSED_CHANNEL_EXCEPTION);
    }

    public final int sendToAddress(long memoryAddress, int pos, int limit, InetAddress addr, int port)
            throws IOException {
        // just duplicate the toNativeInetAddress code here to minimize object creation as this method is expected
        // to be called frequently
        byte[] address;
        int scopeId;
        if (addr instanceof Inet6Address) {
            address = addr.getAddress();
            scopeId = ((Inet6Address) addr).getScopeId();
        } else {
            // convert to ipv4 mapped ipv6 address;
            scopeId = 0;
            address = ipv4MappedIpv6Address(addr.getAddress());
        }
        int res = sendToAddress(fd, memoryAddress, pos, limit, address, scopeId, port);
        if (res >= 0) {
            return res;
        }
        if (res == ERROR_ECONNREFUSED_NEGATIVE) {
            throw new PortUnreachableException("sendToAddress failed");
        }
        return ioResult("sendToAddress", res,
                SEND_TO_ADDRESS_CONNECTION_RESET_EXCEPTION, SEND_TO_ADDRESS_CLOSED_CHANNEL_EXCEPTION);
    }

    public final int sendToAddresses(long memoryAddress, int length, InetAddress addr, int port) throws IOException {
        // just duplicate the toNativeInetAddress code here to minimize object creation as this method is expected
        // to be called frequently
        byte[] address;
        int scopeId;
        if (addr instanceof Inet6Address) {
            address = addr.getAddress();
            scopeId = ((Inet6Address) addr).getScopeId();
        } else {
            // convert to ipv4 mapped ipv6 address;
            scopeId = 0;
            address = ipv4MappedIpv6Address(addr.getAddress());
        }
        int res = sendToAddresses(fd, memoryAddress, length, address, scopeId, port);
        if (res >= 0) {
            return res;
        }

        if (res == ERROR_ECONNREFUSED_NEGATIVE) {
            throw new PortUnreachableException("sendToAddresses failed");
        }
        return ioResult("sendToAddresses", res,
                CONNECTION_RESET_EXCEPTION_SENDMSG, SEND_TO_ADDRESSES_CLOSED_CHANNEL_EXCEPTION);
    }

    public final DatagramSocketAddress recvFrom(ByteBuffer buf, int pos, int limit) throws IOException {
        return recvFrom(fd, buf, pos, limit);
    }

    public final DatagramSocketAddress recvFromAddress(long memoryAddress, int pos, int limit) throws IOException {
        return recvFromAddress(fd, memoryAddress, pos, limit);
    }

    public final int recvFd() throws IOException {
        int res = recvFd(fd);
        if (res > 0) {
            return res;
        }
        if (res == 0) {
            return -1;
        }

        if (res == ERRNO_EAGAIN_NEGATIVE || res == ERRNO_EWOULDBLOCK_NEGATIVE) {
            // Everything consumed so just return -1 here.
            return 0;
        }
        throw newIOException("recvFd", res);
    }

    public final int sendFd(int fdToSend) throws IOException {
        int res = sendFd(fd, fdToSend);
        if (res >= 0) {
            return res;
        }
        if (res == ERRNO_EAGAIN_NEGATIVE || res == ERRNO_EWOULDBLOCK_NEGATIVE) {
            // Everything consumed so just return -1 here.
            return -1;
        }
        throw newIOException("sendFd", res);
    }

    public final boolean connect(SocketAddress socketAddress) throws IOException {
        int res;
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            NativeInetAddress address = NativeInetAddress.newInstance(inetSocketAddress.getAddress());
            res = connect(fd, address.address, address.scopeId, inetSocketAddress.getPort());
        } else if (socketAddress instanceof DomainSocketAddress) {
            DomainSocketAddress unixDomainSocketAddress = (DomainSocketAddress) socketAddress;
            res = connectDomainSocket(fd, unixDomainSocketAddress.path().getBytes(CharsetUtil.UTF_8));
        } else {
            throw new Error("Unexpected SocketAddress implementation " + socketAddress);
        }
        if (res < 0) {
            if (res == ERRNO_EINPROGRESS_NEGATIVE) {
                // connect not complete yet need to wait for EPOLLOUT event
                return false;
            }
            throwConnectException("connect", CONNECT_REFUSED_EXCEPTION, res);
        }
        return true;
    }

    public final boolean finishConnect() throws IOException {
        int res = finishConnect(fd);
        if (res < 0) {
            if (res == ERRNO_EINPROGRESS_NEGATIVE) {
                // connect still in progress
                return false;
            }
            throwConnectException("finishConnect", FINISH_CONNECT_REFUSED_EXCEPTION, res);
        }
        return true;
    }

    public final void disconnect() throws IOException {
        int res = disconnect(fd);
        if (res < 0) {
            throwConnectException("disconnect", FINISH_CONNECT_REFUSED_EXCEPTION, res);
        }
    }

    public final void bind(SocketAddress socketAddress) throws IOException {
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress addr = (InetSocketAddress) socketAddress;
            NativeInetAddress address = NativeInetAddress.newInstance(addr.getAddress());
            int res = bind(fd, address.address, address.scopeId, addr.getPort());
            if (res < 0) {
                throw newIOException("bind", res);
            }
        } else if (socketAddress instanceof DomainSocketAddress) {
            DomainSocketAddress addr = (DomainSocketAddress) socketAddress;
            int res = bindDomainSocket(fd, addr.path().getBytes(CharsetUtil.UTF_8));
            if (res < 0) {
                throw newIOException("bind", res);
            }
        } else {
            throw new Error("Unexpected SocketAddress implementation " + socketAddress);
        }
    }

    public final void listen(int backlog) throws IOException {
        int res = listen(fd, backlog);
        if (res < 0) {
            throw newIOException("listen", res);
        }
    }

    public final int accept(byte[] addr) throws IOException {
        int res = accept(fd, addr);
        if (res >= 0) {
            return res;
        }
        if (res == ERRNO_EAGAIN_NEGATIVE || res == ERRNO_EWOULDBLOCK_NEGATIVE) {
            // Everything consumed so just return -1 here.
            return -1;
        }
        throw newIOException("accept", res);
    }

    public final InetSocketAddress remoteAddress() {
        byte[] addr = remoteAddress(fd);
        // addr may be null if getpeername failed.
        // See https://github.com/netty/netty/issues/3328
        return addr == null ? null : address(addr, 0, addr.length);
    }

    public final InetSocketAddress localAddress() {
        byte[] addr = localAddress(fd);
        // addr may be null if getpeername failed.
        // See https://github.com/netty/netty/issues/3328
        return addr == null ? null : address(addr, 0, addr.length);
    }

    public final int getReceiveBufferSize() throws IOException {
        return getReceiveBufferSize(fd);
    }

    public final int getSendBufferSize() throws IOException {
        return getSendBufferSize(fd);
    }

    public final boolean isKeepAlive() throws IOException {
        return isKeepAlive(fd) != 0;
    }

    public final boolean isTcpNoDelay() throws IOException {
        return isTcpNoDelay(fd) != 0;
    }

    public final boolean isReuseAddress() throws IOException {
        return isReuseAddress(fd) != 0;
    }

    public final boolean isReusePort() throws IOException {
        return isReusePort(fd) != 0;
    }

    public final boolean isBroadcast() throws IOException {
        return isBroadcast(fd) != 0;
    }

    public final int getSoLinger() throws IOException {
        return getSoLinger(fd);
    }

    public final int getSoError() throws IOException {
        return getSoError(fd);
    }

    public final int getTrafficClass() throws IOException {
        return getTrafficClass(fd);
    }

    public final void setKeepAlive(boolean keepAlive) throws IOException {
        setKeepAlive(fd, keepAlive ? 1 : 0);
    }

    public final void setReceiveBufferSize(int receiveBufferSize) throws IOException  {
        setReceiveBufferSize(fd, receiveBufferSize);
    }

    public final void setSendBufferSize(int sendBufferSize) throws IOException {
        setSendBufferSize(fd, sendBufferSize);
    }

    public final void setTcpNoDelay(boolean tcpNoDelay) throws IOException  {
        setTcpNoDelay(fd, tcpNoDelay ? 1 : 0);
    }

    public final void setSoLinger(int soLinger) throws IOException {
        setSoLinger(fd, soLinger);
    }

    public final void setReuseAddress(boolean reuseAddress) throws IOException {
        setReuseAddress(fd, reuseAddress ? 1 : 0);
    }

    public final void setReusePort(boolean reusePort) throws IOException {
        setReusePort(fd, reusePort ? 1 : 0);
    }

    public final void setBroadcast(boolean broadcast) throws IOException {
        setBroadcast(fd, broadcast ? 1 : 0);
    }

    public final void setTrafficClass(int trafficClass) throws IOException {
        setTrafficClass(fd, trafficClass);
    }

    @Override
    public String toString() {
        return "Socket{" +
                "fd=" + fd +
                '}';
    }

    public static Socket newSocketStream() {
        return new Socket(newSocketStream0());
    }

    public static Socket newSocketDgram() {
        return new Socket(newSocketDgram0());
    }

    public static Socket newSocketDomain() {
        return new Socket(newSocketDomain0());
    }

    protected static int newSocketStream0() {
        int res = newSocketStreamFd();
        if (res < 0) {
            throw new ChannelException(newIOException("newSocketStream", res));
        }
        return res;
    }

    protected static int newSocketDgram0() {
        int res = newSocketDgramFd();
        if (res < 0) {
            throw new ChannelException(newIOException("newSocketDgram", res));
        }
        return res;
    }

    protected static int newSocketDomain0() {
        int res = newSocketDomainFd();
        if (res < 0) {
            throw new ChannelException(newIOException("newSocketDomain", res));
        }
        return res;
    }

    private static native int shutdown(int fd, boolean read, boolean write);
    private static native int connect(int fd, byte[] address, int scopeId, int port);
    private static native int connectDomainSocket(int fd, byte[] path);
    private static native int finishConnect(int fd);
    private static native int disconnect(int fd);
    private static native int bind(int fd, byte[] address, int scopeId, int port);
    private static native int bindDomainSocket(int fd, byte[] path);
    private static native int listen(int fd, int backlog);
    private static native int accept(int fd, byte[] addr);

    private static native byte[] remoteAddress(int fd);
    private static native byte[] localAddress(int fd);

    private static native int sendTo(
            int fd, ByteBuffer buf, int pos, int limit, byte[] address, int scopeId, int port);
    private static native int sendToAddress(
            int fd, long memoryAddress, int pos, int limit, byte[] address, int scopeId, int port);
    private static native int sendToAddresses(
            int fd, long memoryAddress, int length, byte[] address, int scopeId, int port);

    private static native DatagramSocketAddress recvFrom(
            int fd, ByteBuffer buf, int pos, int limit) throws IOException;
    private static native DatagramSocketAddress recvFromAddress(
            int fd, long memoryAddress, int pos, int limit) throws IOException;
    private static native int recvFd(int fd);
    private static native int sendFd(int socketFd, int fd);

    private static native int newSocketStreamFd();
    private static native int newSocketDgramFd();
    private static native int newSocketDomainFd();

    private static native int isReuseAddress(int fd) throws IOException;
    private static native int isReusePort(int fd) throws IOException;
    private static native int getReceiveBufferSize(int fd) throws IOException;
    private static native int getSendBufferSize(int fd) throws IOException;
    private static native int isKeepAlive(int fd) throws IOException;
    private static native int isTcpNoDelay(int fd) throws IOException;
    private static native int isBroadcast(int fd) throws IOException;
    private static native int getSoLinger(int fd) throws IOException;
    private static native int getSoError(int fd) throws IOException;
    private static native int getTrafficClass(int fd) throws IOException;

    private static native void setReuseAddress(int fd, int reuseAddress) throws IOException;
    private static native void setReusePort(int fd, int reuseAddress) throws IOException;
    private static native void setKeepAlive(int fd, int keepAlive) throws IOException;
    private static native void setReceiveBufferSize(int fd, int receiveBufferSize) throws IOException;
    private static native void setSendBufferSize(int fd, int sendBufferSize) throws IOException;
    private static native void setTcpNoDelay(int fd, int tcpNoDelay) throws IOException;
    private static native void setSoLinger(int fd, int soLinger) throws IOException;
    private static native void setBroadcast(int fd, int broadcast) throws IOException;
    private static native void setTrafficClass(int fd, int trafficClass) throws IOException;
}
