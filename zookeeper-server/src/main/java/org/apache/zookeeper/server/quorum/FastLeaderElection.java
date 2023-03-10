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

package org.apache.zookeeper.server.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumOracleMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */

public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{} = {} ms", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{} = {} ms", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */ long leader; // 被推举的sid

        /*
         * zxid of the proposed leader
         */ long zxid; // 被推举的zxid

        /*
         * Epoch
         */ long electionEpoch; // 发投票的那个节点的时钟周期

        /*
         * current state of sender
         */ QuorumPeer.ServerState state; // 发投票的那个节点的状态

        /*
         * Address of sender
         */ long sid; // 发来投票的那个节点sid

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */ long peerEpoch; // 被推举的epoch

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    public static class ToSend {

        enum mType {
            crequest,
            challenge,
            notification,
            ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader; // 被推举的sid
            this.zxid = zxid; // 被推举的zxid
            this.electionEpoch = electionEpoch; // 时钟 任期号
            this.state = state; // 消息发送人的状态 选主
            this.sid = sid; // 消息接收者
            this.peerEpoch = peerEpoch; // 被推举的epoch
            this.configData = configData; // 接收方
        }

        /*
         * Proposed leader in the case of notification
         */ long leader; // 被推举的sid

        /*
         * id contains the tag for acks, and zxid for notifications
         */ long zxid; // 被推举的zxid

        /*
         * Epoch
         */ long electionEpoch; // 时钟 任期号

        /*
         * Current state;
         */ QuorumPeer.ServerState state; // 消息发送人的状态 寻主

        /*
         * Address of recipient
         */ long sid; // 消息接收者sid

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData; // 被序列化出来的消息接收方

        /*
         * Leader epoch
         */ long peerEpoch; // 被推举的epoch

    }

    LinkedBlockingQueue<ToSend> sendqueue; // 选票发送队列
    LinkedBlockingQueue<Notification> recvqueue; // 选票接收队

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger { // 负责消息处理

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */
        // 从QuorumCnxManager接收消息并处理消息
        class WorkerReceiver extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        /**
                         * 从QuorumCnxManager的recvQueue超时取接收到的数据 这些数据是Socket接收到的
                         * 也就是其他节点的投票信息
                         */
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        response.buffer.clear(); // 这个地方不是清空buffer 准备读取buffer里面内容 具体看源码

                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        /**
                         * FLE中的sendWorker线程负责处理要发送的投票
                         * 现在FLE的Messenger的wrThread负责接收投票
                         *
                         * |         4          |      8    |      8      |     8     |       8      |       4      |     4    | ?  |
                         * |--------------------|-----------|-------------|-----------|--------------|--------------|---------|-----|
                         * | 消息发送人的状态 选主 | 被推举的sid | 被推举的zxid | 时钟 任期号 | 被推举的epoch | 版本号(固定值) | 数据长度 | 数据 |
                         * 投票里面数据是接收方的序列化
                         */
                        int rstate = response.buffer.getInt(); // 发送投票人的状态 选主
                        long rleader = response.buffer.getLong(); // 被推举的sid
                        long rzxid = response.buffer.getLong(); // 被推举的zxid
                        long relectionEpoch = response.buffer.getLong(); // 时钟 任期号
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null; // 接收方 就是现在的自己

                        try {
                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong(); // 被推举的epoch
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */

                                    version = response.buffer.getInt(); // 固定值版本号 0x02
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt(); // 数据长度

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                                                        response.sid, capacity, version, configLength));
                                }

                                byte[] b = new byte[configLength]; // 投票里面携带的数据内容
                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b, UTF_8)); // 投票里面待的内容是接收方的序列化
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) { // 跳过这个逻辑 不出意外版本号肯定是一样的
                                            LOG.info("{} Received version: {} my version: {}",
                                                     self.getId(),
                                                     Long.toHexString(rqv.getVersion()),
                                                     Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                     response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) { // 被推举的sid就是leader候选人 确认一下在集群候选人中
                            // 有人给a投票当leader 在我维护的有效的集群候选人表中 a不存在 我给a发一个投票 这个投票内容是投自己当leader
                            // 为啥要这样干呢
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(
                                ToSend.mType.notification,
                                current.getId(),
                                current.getZxid(),
                                logicalclock.get(),
                                self.getPeerState(),
                                response.sid, // 消息接收者
                                current.getPeerEpoch(),
                                qv.toString().getBytes(UTF_8));

                            sendqueue.offer(notmsg);
                        } else {
                            // 收到了一张有效投票
                            // Receive new message
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) { // 发来投票的节点状态
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }

                            // 相当于WorkerReceiver线程仅仅是把Message解析出来封装Notification
                            n.leader = rleader; // 被推举的sid
                            n.zxid = rzxid; // 被推举的zxid
                            n.electionEpoch = relectionEpoch; // 发投票的那个节点的时钟周期
                            n.state = ackstate; // 发投票的那个节点的状态
                            n.sid = response.sid; // 投票接收方sid
                            n.peerEpoch = rpeerepoch; // 被推举的epoch
                            n.version = version; // 固定值0x02
                            n.qv = rqv; // 接收方 就是现在的自己
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, "
                                    + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                self.getPeerState(),
                                n.sid,
                                n.state,
                                n.leader,
                                Long.toHexString(n.electionEpoch),
                                Long.toHexString(n.peerEpoch),
                                Long.toHexString(n.zxid),
                                Long.toHexString(n.version),
                                (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) { // 我还在寻主
                                recvqueue.offer(n); // 先把选票给FLE算法

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                    && (n.electionEpoch < logicalclock.get())) { // 当初发送投票的那台机器的时钟epoch已经落后了 说明这张选票已经没有竞争力了
                                    /**
                                     * 这张投票的机器时钟已经落后了 在我这都已经没有竞争力了 但是这张票我不作废 我帮投票人更新一下机器时钟继续发送给我自己
                                     * 但是这样的话对于我而言 不就是会在FLE的recvqueue中收到两次吗
                                     */
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        v.getId(),
                                        v.getZxid(),
                                        logicalclock.get(), // 我的机器时钟
                                        self.getPeerState(), // 我的状态 选主
                                        response.sid, // 这不就是我自己吗
                                        v.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else { // 我并不寻主 也就是我可能是Leader或者Follower或者Observer
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                        "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                        self.getId(),
                                        response.sid,
                                        Long.toHexString(current.getZxid()),
                                        current.getId(),
                                        Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                        ToSend.mType.notification,
                                        current.getId(),
                                        current.getZxid(),
                                        current.getElectionEpoch(),
                                        self.getPeerState(),
                                        response.sid,
                                        current.getPeerEpoch(),
                                        qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */
        // 负责将要发送的消息出队并放到QuorumCnxManager的队列
        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        // FLE算法的投票都在sendqueue中
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }
                        /**
                         * FLE中的sendWorker线程负责处理要发送的投票
                         *
                         * |         4          |      8    |      8      |     8     |       8      |       4      |     4    | ?  |
                         * |--------------------|-----------|-------------|-----------|--------------|--------------|---------|-----|
                         * | 消息发送人的状态 选主 | 被推举的sid | 被推举的zxid | 时钟 任期号 | 被推举的epoch | 版本号(固定值) | 数据长度 | 数据 |
                         */
                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                /**
                 * state->消息发送人的状态 寻主
                 * leader->被推举的sid
                 * zxid->被推举的zxid
                 * electionEpoch->时钟 任期号
                 * peerEpoch->被推举的epoch
                 * configData->序列化出来的消息接收方
                 */
                /**
                 * |         4          |      8    |      8      |     8     |       8      |       4      |     4    | ?  |
                 * |--------------------|-----------|-------------|-----------|--------------|--------------|---------|-----|
                 * | 消息发送人的状态 选主 | 被推举的sid | 被推举的zxid | 时钟 任期号 | 被推举的epoch | 版本号(固定值) | 数据长度 | 数据 |
                 */
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);

                /**
                 * 发送给谁->消息接收方的sid
                 * 发送什么数据->封装号的投票
                 * 封装好的数据协议通QuorumCxnManager发送出去
                 */
                manager.toSend(m.sid, requestBuffer);

            }

        }

        /**
         * 由wsThread发送线程执行
         * 选票发送线程
         * 将FastLeaderElection的ToSend转化为QuorumCnxManager的Message
         * 不断从sendqueue中获取待发送的选票 传递给QuorumCnxManager
         */
        WorkerSender ws;

        /**
         * 由wrThread接收线程执行
         * 选票接收线程
         * 将QuorumCnxManager的Message转换FastLeaderElection的Notification
         * 不断地从QuorumCnxManager中获取其他服务器发送来的选举消息
         * 将选举消息转换为一个选票放到recvqueue中
         * 在选票接收过程中 如果发现外部选票的选举轮次小于当前服务器的 就将外部的选票忽略掉
         */
        WorkerReceiver wr;
        // 发送线程
        Thread wsThread = null;
        // 接收线程
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            /**
             * 线程
             * 该线程被CPU调度起来后会执行ws这个任务
             */
            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            /**
             * 线程
             * 该线程被CPU调度起来后会执行wr这个任务
             */
            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            /**
             * 启动两个线程
             *   - wsThread发送线程 负责执行ws发送任务
             *   - wrThread接收线程 负责执行wr接收任务
             */
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader; // 选举谁当leader 它的sid
    long proposedZxid; // 选举谁当leader 它的zxid
    long proposedEpoch; // 选举谁当leader 它的epoch

    /**
     * Returns the current value of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        /**
         * |         4          |      8    |      8      |     8     |       8      |       4      |     4    | ?  |
         * |--------------------|-----------|-------------|-----------|--------------|--------------|---------|-----|
         * | 消息发送人的状态 选主 | 被推举的sid | 被推举的zxid | 时钟 任期号 | 被推举的epoch | 版本号(固定值) | 数据长度 | 数据 |
         */
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state); // 消息发送人的状态 寻主
        requestBuffer.putLong(leader); // 被推举的sid
        requestBuffer.putLong(zxid); // 被推举的zxid
        requestBuffer.putLong(electionEpoch); // 时钟 任期号
        requestBuffer.putLong(epoch); // 被推举的epoch
        requestBuffer.putInt(Notification.CURRENTVERSION); // 版本号
        requestBuffer.putInt(configData.length); // 数据长度
        requestBuffer.put(configData); // 数据

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>(); // 发送队列
        recvqueue = new LinkedBlockingQueue<Notification>(); // 接收队列
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        /**
         * 启动messenger中的两个线程
         *   - wsThread发送线程 负责执行ws发送任务
         *   - wrThread接收线程 负责执行wr接收任务
         */
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug(
            "About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
            v.getId(),
            Long.toHexString(v.getZxid()),
            self.getId(),
            self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;
    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        // 给集群中参与选主投票的所有节点都发送通知
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            ToSend notmsg = new ToSend(
                ToSend.mType.notification, // 消息类型 通知
                proposedLeader, // 被推举的sid
                proposedZxid, // 被推举的zxid
                logicalclock.get(), // 时钟 任期号
                QuorumPeer.ServerState.LOOKING, // 发消息人的状态 寻主
                sid, // 消息接收者
                proposedEpoch, // 被推举的epoch
                qv.toString().getBytes(UTF_8)); // 接收方

            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug(
            "id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}",
            newId,
            curId,
            Long.toHexString(newZxid),
            Long.toHexString(curZxid));

        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        /**
         * 选票候选人pk
         * 比较优先级
         *   - epoch
         *   - zxid
         *   - sid
         */
        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                    && ((newZxid > curZxid)
                        || ((newZxid == curZxid)
                            && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
            && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) { // 投票箱里面的票根
            if (vote.equals(entry.getValue())) { // 跟我投一样leader的是谁 记在qvAckSet里面
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != self.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    /**
     * 选举谁当leader 它的信息
     *   - 它的sid
     *   - 它的zxid
     *   - 它的epoch
     */
    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug(
            "Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)",
            leader,
            Long.toHexString(zxid),
            proposedLeader,
            Long.toHexString(proposedZxid));

        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();
        self.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce on whether a majority
             * of participants has voted for it.
             */
            // 存储当前选举接收到的票据
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             */
            // 存储上次选举的票据
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = minNotificationInterval;

            synchronized (this) {
                // 逻辑时钟自增 用来判断票据是否在同一轮选举
                logicalclock.incrementAndGet();
                /**
                 * 选举谁当leader
                 * 选自己当leader
                 * 就是把被推荐当leader的信息记在FLE算法中
                 */
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            // 当票据发生变更就异步发送通知告知所有竞选节点
            sendNotifications();

            SyncedLearnerTracker voteSet = null;

            /*
             * Loop in which we exchange notifications until we find a leader
             */

            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) { // 直至竞选出Leader才选结束选举 一旦选主成功那么曾经参与选主的节点要么是Leader要么是Follower 状态不可能再是LOOKING
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                /**
                 * FLE算法收到的投票
                 *   - 自己投自己的那一票
                 *   - 别的节点的投票(投谁不知道)
                 */
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if (n == null) { // FLE投票箱没有投票
                    if (manager.haveDelivered()) { // 当前节点没有待发送投票
                        sendNotifications(); // 再次向节点发送一下自己的投票(自己投自己当Leader)
                    } else {
                        manager.connectAll(); // 尝试和每个节点建立连接
                    }

                    /*
                     * Exponential backoff
                     */
                    notTimeout = Math.min(notTimeout << 1, maxNotificationInterval);

                    /*
                     * When a leader failure happens on a master, the backup will be supposed to receive the honour from
                     * Oracle and become a leader, but the honour is likely to be delay. We do a re-check once timeout happens
                     *
                     * The leader election algorithm does not provide the ability of electing a leader from a single instance
                     * which is in a configuration of 2 instances.
                     * */
                    if (self.getQuorumVerifier() instanceof QuorumOracleMaj
                            && self.getQuorumVerifier().revalidateVoteset(voteSet, notTimeout != minNotificationInterval)) {
                        setPeerState(proposedLeader, voteSet);
                        Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                        leaveInstance(endVote);
                        return endVote;
                    }

                    LOG.info("Notification time out: {} ms", notTimeout);

                } else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    switch (n.state) { // 判断投票者的状态 如果是LOOKING说明也在找Leader
                    case LOOKING: // 发投票的那个节点也在寻主
                        if (getInitLastLoggedZxid() == -1) { // 只要ZKDatabase初始化过zxid的默认值就是0 处理过事务之后这个zxid还是自增 所以-1肯定是异常的
                            LOG.debug("Ignoring notification as our zxid is -1");
                            break;
                        }
                        if (n.zxid == -1) { // 同理ZKDatabase初始化过zxid的默认值就是0 -1异常
                            LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                            break;
                        }
                        // If notification > current, replace and send messages out
                        if (n.electionEpoch > logicalclock.get()) {
                            /**
                             * 发来投票的那个节点的时钟周期比当前节点大 说明当前节点时钟落后了 已经不在一个选举轮次上了
                             * 自己选谁都是没有意义的 发到别人那边的投票直接被丢掉了
                             */
                            logicalclock.set(n.electionEpoch); // 先更新自己的时钟
                            recvset.clear(); // 清空之前收集的外部投票箱 因为投票箱是在特定时钟周期下的凭证 没有意义了

                            /**
                             * 结合收到的别人在有效时钟周期下的投票
                             * 参考它的推荐
                             * 自己也就有了推荐人
                             * 把自己的推荐消息广播出去
                             */
                            if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                            }
                            sendNotifications();
                        } else if (n.electionEpoch < logicalclock.get()) {
                            /**
                             * 发来投票的那个节点的时钟周期比当前节点小 投票作废
                             * 别人的时钟周期已经落后 选谁都没有意义 结束算法流程
                             */
                                LOG.debug(
                                    "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                    Long.toHexString(n.electionEpoch),
                                    Long.toHexString(logicalclock.get()));
                            break;
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) { // 发投票的节点的时钟周期和自己处在一个轮次 最简单 直接pk它的推举和自己的推举
                            /**
                             * 两个推举进行pk
                             * 无论那个候选人胜出 都更新自己现在的主观推荐
                             * 然后把最新的选择告知集群其他候选人
                             */
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            sendNotifications();
                        }

                        LOG.debug(
                            "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                            n.sid,
                            n.leader,
                            Long.toHexString(n.zxid),
                            Long.toHexString(n.electionEpoch));

                        // don't care about the version if it's in LOOKING state
                        /**
                         * 首先 代码能执行到这说明
                         *   - 外来选票是有效的
                         *   - 这个外来选票包含了自己给自己投的
                         * 把外来的有效的通知转换成投票 放到投票箱recvset中
                         * 也就是把外部投票进行归档
                         * 它的用途是啥呢
                         *   - value是选票 也就是谁可以当leader
                         *   - key是谁投了选票
                         * 那么就可以对投票箱进行汇总得出
                         *   - 哪些人被投为了leader
                         *   - 支持当leader的数量
                         */
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                        /**
                         * 下面两个步骤合起来看比较容易理解
                         *   - 首先 在当前FLE算法中维护的LEADER候选人信息就是最后可能当leader的人
                         *     - proposedLeader
                         *     - proposedZxid
                         *     - proposedEpoch
                         *     因为上次网络有通知消息进来都会比较pk 将pk胜出的更新为最新的这几个阈值 然后才会再将投票归档
                         *     也就是说所有的投票中如果真的已经可以结算出leader 那么也只有可能是现在算法维护的proposedLeader
                         *   - 有了这个共识之后 事情就变得简单了
                         *     - 因为已经有了leader的得力候选人
                         *     - 拿着投票归档箱去看都有哪些人投了proposedLeader为leader的 把他们记下来
                         *     - 投了proposedLeader的人数超过了集群中参与选主人数一半就结算出leader了
                         */
                        voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch)); // 从投票归档箱中统计还有谁投了leader得力候选人
                        if (voteSet.hasAllQuorums()) {
                            /**
                             * 集群中参与选主有过半人都投proposedLeader当leader了 选主初步完成了
                             * 此时leader已经决胜出来了
                             *
                             * 下面超时方式看看有没有投票进来
                             *
                             * 有一种场景就是比如集群共3个记点 依次启动1 2 3
                             * 先启动1 再1启动2 就已经可以判定2为leader了
                             * 此刻3再启动
                             * 这个场景就可以触发下面的这种情况
                             */

                            // Verify if there is any change in the proposed leader
                            while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) { // 看看还有没有投票
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                    recvqueue.put(n);
                                    break;
                                }
                            }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                            // 200ms内没有新的投票 结束投票
                            if (n == null) {
                                /**
                                 * proposedLeader就是集群leader
                                 * 更新节点的状态
                                 *   - Leader是自己 就直接更新
                                 *   - 自己不是Leader就根据节点特性更新为Follower或者Observer
                                 */
                                setPeerState(proposedLeader, voteSet);
                                // 最终的投票
                                Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                // 当前投票阶段已经绝胜出leader 投票归档箱已经没有用了 清空它
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        break;
                    case OBSERVING:
                        LOG.debug("Notification from observer: {}", n.sid);
                        break;

                        /*
                        * In ZOOKEEPER-3922, we separate the behaviors of FOLLOWING and LEADING.
                        * To avoid the duplication of codes, we create a method called followingBehavior which was used to
                        * shared by FOLLOWING and LEADING. This method returns a Vote. When the returned Vote is null, it follows
                        * the original idea to break swtich statement; otherwise, a valid returned Vote indicates, a leader
                        * is generated.
                        *
                        * The reason why we need to separate these behaviors is to make the algorithm runnable for 2-node
                        * setting. An extra condition for generating leader is needed. Due to the majority rule, only when
                        * there is a majority in the voteset, a leader will be generated. However, in a configuration of 2 nodes,
                        * the number to achieve the majority remains 2, which means a recovered node cannot generate a leader which is
                        * the existed leader. Therefore, we need the Oracle to kick in this situation. In a two-node configuration, the Oracle
                        * only grants the permission to maintain the progress to one node. The oracle either grants the permission to the
                        * remained node and makes it a new leader when there is a faulty machine, which is the case to maintain the progress.
                        * Otherwise, the oracle does not grant the permission to the remained node, which further causes a service down.
                        *
                        * In the former case, when a failed server recovers and participate in the leader election, it would not locate a
                        * new leader because there does not exist a majority in the voteset. It fails on the containAllQuorum() infinitely due to
                        * two facts. First one is the fact that it does do not have a majority in the voteset. The other fact is the fact that
                        * the oracle would not give the permission since the oracle already gave the permission to the existed leader, the healthy machine.
                        * Logically, when the oracle replies with negative, it implies the existed leader which is LEADING notification comes from is a valid leader.
                        * To threat this negative replies as a permission to generate the leader is the purpose to separate these two behaviors.
                        *
                        *
                        * */
                    case FOLLOWING:
                        /*
                        * To avoid duplicate codes
                        * */
                        Vote resultFN = receivedFollowingNotification(recvset, outofelection, voteSet, n);
                        if (resultFN == null) {
                            break;
                        } else {
                            return resultFN;
                        }
                    case LEADING: // 收到的票据显示那个节点状态已经是LEADING了
                        /*
                        * In leadingBehavior(), it performs followingBehvior() first. When followingBehavior() returns
                        * a null pointer, ask Oracle whether to follow this leader.
                        * */
                        Vote resultLN = receivedLeadingNotification(recvset, outofelection, voteSet, n);
                        if (resultLN == null) {
                            break;
                        } else {
                            return resultLN;
                        }
                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {}(n.sid)", n.state, n.sid);
                        break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    private Vote receivedFollowingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
         * Consider all notifications from the same epoch
         * together.
         */
        if (n.electionEpoch == logicalclock.get()) {
            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            voteSet = getVoteTracker(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            if (voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {
                setPeerState(n.leader, voteSet);
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                leaveInstance(endVote);
                return endVote;
            }
        }

        /*
         * Before joining an established ensemble, verify that
         * a majority are following the same leader.
         *
         * Note that the outofelection map also stores votes from the current leader election.
         * See ZOOKEEPER-1732 for more information.
         */
        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
        voteSet = getVoteTracker(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));

        if (voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {
            synchronized (this) {
                logicalclock.set(n.electionEpoch);
                setPeerState(n.leader, voteSet);
            }
            Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
            leaveInstance(endVote);
            return endVote;
        }

        return null;
    }

    private Vote receivedLeadingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
        *
        * In a two-node configuration, a recovery nodes cannot locate a leader because of the lack of the majority in the voteset.
        * Therefore, it is the time for Oracle to take place as a tight breaker.
        *
        * */
        Vote result = receivedFollowingNotification(recvset, outofelection, voteSet, n);
        if (result == null) {
            /*
            * Ask Oracle to see if it is okay to follow this leader.
            *
            * We don't need the CheckLeader() because itself cannot be a leader candidate
            * */
            if (self.getQuorumVerifier().getNeedOracle() && !self.getQuorumVerifier().askOracle()) {
                LOG.info("Oracle indicates to follow");
                setPeerState(n.leader, voteSet);
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                leaveInstance(endVote);
                return endVote;
            } else {
                LOG.info("Oracle indicates not to follow");
                return null;
            }
        } else {
            return result;
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
