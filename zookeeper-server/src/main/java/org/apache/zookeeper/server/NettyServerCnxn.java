/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NettyServerCnxn维护了服务器与客户端之间的通道缓冲、缓冲区以及会话
 * 使用Netty来处理服务端与客户端的通信
 */
public class NettyServerCnxn extends ServerCnxn {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServerCnxn.class);

    // 客户端与服务端之间的连接通道
    private final Channel channel;

    // 通道数据缓冲队列
    private CompositeByteBuf queuedBuffer;
    // 节流标识
    private final AtomicBoolean throttled = new AtomicBoolean(false);

    // 字节缓冲区 真正用来接收Netty中数据的
    private ByteBuffer bb;

    // 4字节缓冲区 4byte的ByteBuffer专门用来读取数据包中前4字节所记录的数据包中实际数据长度
    private final ByteBuffer bbLen = ByteBuffer.allocate(4);

    // 会话id
    private long sessionId;

    // 会话超时时间 当会话超时时自动释放资源并结束会话
    private int sessionTimeout;
    private Certificate[] clientChain;
    private volatile boolean closingChannel;

    // 会话工厂
    private final NettyServerCnxnFactory factory;

    // 初始化标识
    private boolean initialized;

    public int readIssuedAfterReadComplete;

    // 握手状态 默认为未连接
    private volatile HandshakeState handshakeState = HandshakeState.NONE;

    public enum HandshakeState {
        NONE, // 未连接
        STARTED, // 开始连接
        FINISHED // 完成连接
    }

    NettyServerCnxn(Channel channel, ZooKeeperServer zks, NettyServerCnxnFactory factory) {
        super(zks); // 持有zk实例 将来把netty数据转交给zk
        this.channel = channel;
        this.closingChannel = false;
        this.factory = factory; // 会话工厂 将来关闭会话需要依赖它
        if (this.factory.login != null) { // 用户登陆
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        InetAddress addr = ((InetSocketAddress) channel.remoteAddress()).getAddress();
        addAuthInfo(new Id("ip", addr.getHostAddress())); // 认证信息中添加ip地址
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close(DisconnectReason reason) {
        disconnectReason = reason;
        close();
    }

    // 关闭连接通道
    public void close() {
        closingChannel = true; // 标识符

        LOG.debug("close called for session id: 0x{}", Long.toHexString(sessionId));

        setStale();

        // ZOOKEEPER-2743:
        // Always unregister connection upon close to prevent
        // connection bean leak under certain race conditions.
        factory.unregisterConnection(this); // 取消jmx注册连接 防止连接Bean泄露

        // if this is not in cnxns then it's already closed
        if (!factory.cnxns.remove(this)) { // 移除缓存
            LOG.debug("cnxns size:{}", factory.cnxns.size());
            if (channel.isOpen()) {
                channel.close();
            }
            return;
        }

        LOG.debug("close in progress for session id: 0x{}", Long.toHexString(sessionId));

        factory.removeCnxnFromSessionMap(this);

        factory.removeCnxnFromIpMap(this, ((InetSocketAddress) channel.remoteAddress()).getAddress());

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (channel.isOpen()) { // 连接还没关闭
            // Since we don't check on the futures created by write calls to the channel complete we need to make sure
            // that all writes have been completed before closing the channel or we risk data loss
            // See: http://lists.jboss.org/pipermail/netty-users/2009-August/001122.html
            // 确保通道关闭之前所有写入都已经完成
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    future.channel().close().addListener(f -> releaseQueuedBuffer());
                }
            });
        } else {
            ServerMetrics.getMetrics().CONNECTION_DROP_COUNT.add(1);
            channel.eventLoop().execute(this::releaseQueuedBuffer);
        }
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    // 处理被NettyServerCnxn监听的时间
    @Override
    public void process(WatchedEvent event) {
        // 响应头
        ReplyHeader h = new ReplyHeader(ClientCnxn.NOTIFICATION_XID, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                "Deliver event " + event + " to 0x" + Long.toHexString(this.sessionId) + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper(); // 将WatchEvent转换为可以通过线路发送的类型

        try {
            // 发送响应
            int responseSize = sendResponse(h, e, "notification");
            ServerMetrics.getMetrics().WATCH_BYTES.add(responseSize);
        } catch (IOException e1) {
            LOG.debug("Problem sending to {}", getRemoteSocketAddress(), e1);
            close();
        }
    }

    // 给客户端发送响应
    @Override
    public int sendResponse(ReplyHeader h, Record r, String tag,
                             String cacheKey, Stat stat, int opCode) throws IOException {
        // cacheKey and stat are used in caching, which is not
        // implemented here. Implementation example can be found in NIOServerCnxn.
        if (closingChannel || !channel.isOpen()) {
            return 0;
        }
        ByteBuffer[] bb = serialize(h, r, tag, cacheKey, stat, opCode);
        int responseSize = bb[0].getInt();
        bb[0].rewind();
        sendBuffer(bb);
        decrOutstandingAndCheckThrottle(h);
        return responseSize;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    // Use a single listener instance to reduce GC
    private final GenericFutureListener<Future<Void>> onSendBufferDoneListener = f -> {
        if (f.isSuccess()) {
            packetSent();
        }
    };

    @Override
    public void sendBuffer(ByteBuffer... buffers) {
        if (buffers.length == 1 && buffers[0] == ServerCnxnFactory.closeConn) {
            close(DisconnectReason.CLIENT_CLOSED_CONNECTION);
            return;
        }
        channel.writeAndFlush(Unpooled.wrappedBuffer(buffers)).addListener(onSendBufferDoneListener);
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {

        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) { // 是否准备好发送另一块
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBuffer(ByteBuffer.wrap(sb.toString().getBytes(UTF_8)));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) {
                return;
            }
            checkFlush(true); // 关闭之前需要强制性发送缓存数据
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }

    }

    /** Return if four letter word found and responded to, otw false **/
    private boolean checkFourLetterWord(final Channel channel, ByteBuf message, final int len) {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);

        // Stops automatic reads of incoming data on this channel. We don't
        // expect any more traffic from the client when processing a 4LW
        // so this shouldn't break anything.
        channel.config().setAutoRead(false);
        packetReceived(4);

        final PrintWriter pwriter = new PrintWriter(new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(
                pwriter,
                this,
                cmd + " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing {} command from {}", cmd, channel.remoteAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            ByteBuffer mask = ByteBuffer.allocate(8);
            message.readBytes(mask);
            mask.flip();
            long traceMask = mask.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /**
     * Helper that throws an IllegalStateException if the current thread is not
     * executing in the channel's event loop thread.
     * @param callerMethodName the name of the calling method to add to the exception message.
     */
    private void checkIsInEventLoop(String callerMethodName) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException(callerMethodName + "() called from non-EventLoop thread");
        }
    }

    /**
     * Appends <code>buf</code> to <code>queuedBuffer</code>. Does not duplicate <code>buf</code>
     * or call any flavor of {@link ByteBuf#retain()}. Caller must ensure that <code>buf</code>
     * is not owned by anyone else, as this call transfers ownership of <code>buf</code> to the
     * <code>queuedBuffer</code>.
     *
     * This method should only be called from the event loop thread.
     * @param buf the buffer to append to the queue.
     */
    private void appendToQueuedBuffer(ByteBuf buf) {
        checkIsInEventLoop("appendToQueuedBuffer");
        if (queuedBuffer.numComponents() == queuedBuffer.maxNumComponents()) {
            // queuedBuffer has reached its component limit, so combine the existing components.
            queuedBuffer.consolidate();
        }
        queuedBuffer.addComponent(true, buf);
        ServerMetrics.getMetrics().NETTY_QUEUED_BUFFER.add(queuedBuffer.capacity());
    }

    /**
     * Process incoming message. This should only be called from the event
     * loop thread.
     * Note that this method does not call <code>buf.release()</code>. The caller
     * is responsible for making sure the buf is released after this method
     * returns.
     * @param buf the message bytes to process.
     */
    void processMessage(ByteBuf buf) {
        checkIsInEventLoop("processMessage");

        /**
         * 开启了节流机制 queuedBuffer缓冲队列就会被实例话
         */
        if (throttled.get()) { // 节流的场景下把数据放到队列里
            // we are throttled, so we need to queue
            if (queuedBuffer == null) {
                LOG.debug("allocating queue");
                queuedBuffer = channel.alloc().compositeBuffer();
            }
            appendToQueuedBuffer(buf.retainedDuplicate());
        } else { // 不节流直接对数据进行读取
            if (queuedBuffer != null) {
                appendToQueuedBuffer(buf.retainedDuplicate());
                processQueuedBuffer(); // 调用->receiveMessage方法
            } else {
                receiveMessage(buf);
                // Have to check !closingChannel, because an error in
                // receiveMessage() could have led to close() being called.
                if (!closingChannel && buf.isReadable()) {
                    if (queuedBuffer == null) {
                        queuedBuffer = channel.alloc().compositeBuffer();
                    }
                    appendToQueuedBuffer(buf.retainedSlice(buf.readerIndex(), buf.readableBytes()));
                }
            }
        }
    }

    /**
     * Try to process previously queued message. This should only be called
     * from the event loop thread.
     */
    void processQueuedBuffer() {
        checkIsInEventLoop("processQueuedBuffer");
        if (queuedBuffer != null) {
            receiveMessage(queuedBuffer);
            if (closingChannel) {
                // close() could have been called if receiveMessage() failed
                LOG.debug("Processed queue - channel closed, dropping remaining bytes");
            } else if (!queuedBuffer.isReadable()) {
                LOG.debug("Processed queue - no bytes remaining");
                releaseQueuedBuffer();
            } else {
                LOG.debug("Processed queue - bytes remaining");
                // Try to reduce memory consumption by freeing up buffer space
                // which is no longer needed.
                queuedBuffer.discardReadComponents();
            }
        } else {
            LOG.debug("queue empty");
        }
    }

    /**
     * Clean up queued buffer once it's no longer needed. This should only be
     * called from the event loop thread.
     */
    private void releaseQueuedBuffer() {
        checkIsInEventLoop("releaseQueuedBuffer");
        if (queuedBuffer != null) {
            queuedBuffer.release();
            queuedBuffer = null;
        }
    }

    /**
     * Receive a message, which can come from the queued buffer or from a new
     * buffer coming in over the channel. This should only be called from the
     * event loop thread.
     * Note that this method does not call <code>message.release()</code>. The
     * caller is responsible for making sure the message is released after this
     * method returns.
     * @param message the message bytes to process.
     */
    private void receiveMessage(ByteBuf message) {
        checkIsInEventLoop("receiveMessage");
        /**
         * 为啥这个方法要设计这么麻烦
         *   - 直观上看就是将message拷贝到bb缓冲区上 然后处理的是bb缓冲区
         *   - 为什么不直接处理message消息
         * 如果这个方法的入口只有Netty read(...)来触发那是完全没必要进行数据拷贝的 可以直接处理mesage
         * 但是这个方法的入口还有queuedBuffer 队列里面数据被聚合过 没有消息边界 需要手工处理判定真实数据多长
         * 而这个方法的启用条件是使用节流机制
         * 也就是说这个方法完全是为了兼容节流+缓冲队列这个场景
         *
         * 方法逻辑很清晰
         * 轮询读取数据包 直到数据包被读完
         *   - 数据包首次进入这个方法时 还没有给数据包分配过缓冲区bb
         *     - 数据包前4字节标识这个数据包中有多长数据要接收 读出来这个长度
         *     - 按照数据包中真实数据长度分配一个长度刚好的缓冲区bb
         *   - 上一轮仅仅读走了数据包前4个字节
         *     - 现在面对的就是真实数据了
         *     - 并且缓冲区bb已经在上一轮中分配好
         *     - 把真实数据一次性全部读走到缓冲区
         */
        try {
            while (message.isReadable() && !throttled.get()) {
                if (bb != null) {
                    // 调整缓冲区大小
                    if (bb.remaining() > message.readableBytes()) {
                        int newLimit = bb.position() + message.readableBytes();
                        bb.limit(newLimit);
                    }
                    // 从数据包中读取长度为bb的ByteBuffer
                    message.readBytes(bb);
                    bb.limit(bb.capacity());

                    if (bb.remaining() == 0) { // 读完了一个完整的message数据包了
                        bb.flip();
                        packetReceived(4 + bb.remaining());

                        ZooKeeperServer zks = this.zkServer;
                        if (zks == null || !zks.isRunning()) {
                            throw new IOException("ZK down");
                        }
                        if (initialized) {
                            // TODO: if zks.processPacket() is changed to take a ByteBuffer[],
                            // we could implement zero-copy queueing.
                            zks.processPacket(this, bb); // 处理数据包中的实际数据
                        } else {
                            zks.processConnectRequest(this, bb); // 连接请求
                            initialized = true;
                        }
                        bb = null; // 又置为null 等待下一个新的数据包进来重新根据数据包前4字节指示的大小分配
                    }
                } else {
                    if (message.readableBytes() < bbLen.remaining()) {
                        bbLen.limit(bbLen.position() + message.readableBytes());
                    }
                    message.readBytes(bbLen); // 读取数据包中前4字节记录的数据包中实际数据长度
                    bbLen.limit(bbLen.capacity()); // 重置bbLen的limit
                    if (bbLen.remaining() == 0) { // 保证bbLen至多读取了message的前4字节内容
                        bbLen.flip();

                        int len = bbLen.getInt(); // 前4字节代表的值 就是数据长度

                        bbLen.clear(); // bbLen使命完成
                        if (!initialized) {
                            if (checkFourLetterWord(channel, message, len)) { // 是否是4字母指令
                                return;
                            }
                        }
                        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
                            throw new IOException("Len error " + len);
                        }
                        ZooKeeperServer zks = this.zkServer;
                        if (zks == null || !zks.isRunning()) {
                            close(DisconnectReason.IO_EXCEPTION);
                            return;
                        }
                        // checkRequestSize will throw IOException if request is rejected
                        zks.checkRequestSizeWhenReceivingMessage(len); // 根据len重新分配缓冲以便接收内容
                        bb = ByteBuffer.allocate(len); // 分配缓冲准备用来接收实际数据
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn("Closing connection to {}", getRemoteSocketAddress(), e);
            close(DisconnectReason.IO_EXCEPTION);
        } catch (ClientCnxnLimitException e) {
            // Common case exception, print at debug level
            ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);

            LOG.debug("Closing connection to {}", getRemoteSocketAddress(), e);
            close(DisconnectReason.CLIENT_RATE_LIMIT);
        }
    }

    /**
     * An event that triggers a change in the channel's read setting.
     * Used for throttling. By using an enum we can treat the two values as
     * singletons and compare with ==.
     */
    enum ReadEvent {
        DISABLE,
        ENABLE
    }

    /**
     * Note that the netty implementation ignores the <code>waitDisableRecv</code>
     * parameter and is always asynchronous.
     * @param waitDisableRecv ignored by this implementation.
     */
    @Override
    public void disableRecv(boolean waitDisableRecv) {
        if (throttled.compareAndSet(false, true)) {
            LOG.debug("Throttling - disabling recv {}", this);
            channel.pipeline().fireUserEventTriggered(ReadEvent.DISABLE);
        }
    }

    @Override
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            LOG.debug("Sending unthrottle event {}", this);
            channel.pipeline().fireUserEventTriggered(ReadEvent.ENABLE);
        }
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public int getInterestOps() {
        // This might not be 100% right, but it's only used for printing
        // connection info in the netty implementation so it's probably ok.
        if (channel == null || !channel.isOpen()) {
            return 0;
        }
        int interestOps = 0;
        if (!throttled.get()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (!channel.isWritable()) {
            // OP_READ means "can read", but OP_WRITE means "cannot write",
            // it's weird.
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    /** Send close connection packet to the client.
     */
    @Override
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return factory.secure;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        if (clientChain == null) {
            return null;
        }
        return Arrays.copyOf(clientChain, clientChain.length);
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        if (chain == null) {
            clientChain = null;
        } else {
            clientChain = Arrays.copyOf(chain, chain.length);
        }
    }

    // For tests and NettyServerCnxnFactory only, thus package-private.
    Channel getChannel() {
        return channel;
    }

    public int getQueuedReadableBytes() {
        checkIsInEventLoop("getQueuedReadableBytes");
        if (queuedBuffer != null) {
            return queuedBuffer.readableBytes();
        }
        return 0;
    }

    public void setHandshakeState(HandshakeState state) {
        this.handshakeState = state;
    }

    public HandshakeState getHandshakeState() {
        return this.handshakeState;
    }
}
