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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.common.Time;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 */
public class ExpiryQueue<E> {

    /**
     * key->会话对象
     * value->归一化后的超时时间
     */
    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     *
     * 以超时时间为key的缓存
     * key->归一化超时时间
     * value->在该时间过期的会话集合
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong(); // 迭代维护下一个过期时间 这样就可以拿着过期时间直接去hash表去这个过期时间映射的连接

    /**
     * 相当于是基数
     * 假定expirationInterval = 10000，
     * connection_1_timeout_point = 1599715479084
     * connection_2_timeout_point =  1599715479184
     * connection_3_timeout_point =  1599715479384
     * 那么通过归一化计算他们三个的超时时间点都变成了1599715480000
     */
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     * @param elem  element to remove
     * @return time at which the element was set to expire, or null if
     *              it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * @param elem     element to add/update
     * @param timeout  timout in milliseconds
     * @return time at which the element is now set to expire if
     *                 changed, or null if unchanged
     */
    public Long update(E elem, int timeout) { // timeout是连接还有多久过期
        Long prevExpiryTime = elemMap.get(elem); // 当前连接维护的当前的过期时间
        long now = Time.currentElapsedTime();
        Long newExpiryTime = roundToNextInterval(now + timeout); // 归一化出来的新的过期时间

        if (newExpiryTime.equals(prevExpiryTime)) { // 不用更新过期时间
            // No change, so nothing to update
            return null;
        }

        // First add the elem to the new expiry time bucket in expiryMap.
        Set<E> set = expiryMap.get(newExpiryTime); // 集合里面都是这个时间过期
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get(); // 下一个过期时间
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     *         ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        if (now < expirationTime) {
            return Collections.emptySet(); // 会话都还没过期
        }

        Set<E> set = null;
        long newExpirationTime = expirationTime + expirationInterval; // expirationInterval归一化的设计确保了所有过期时间都是这个基数的整数倍 因此每次更新过期时间都以这个基准为步进值
        if (nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {
            set = expiryMap.remove(expirationTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -&gt; elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }

}

