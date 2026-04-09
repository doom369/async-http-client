/*
 *    Copyright (c) 2014-2026 AsyncHttpClient Project. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.asynchttpclient.netty.channel;

import io.netty.util.AttributeKey;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Tracks per-connection HTTP/2 state: active stream count, max concurrent streams,
 * draining status (from GOAWAY), and pending stream openers.
 * <p>
 * All mutable state is protected by synchronization on {@code this} to prevent
 * race conditions between stream acquisition, release, and pending opener management.
 */
public class Http2ConnectionState {

    public static final AttributeKey<Http2ConnectionState> HTTP2_STATE_KEY =
            AttributeKey.valueOf("http2ConnectionState");

    /**
     * Represents a pending stream open request that can either be run (when a slot opens)
     * or failed (when the connection is draining).
     */
    public static class PendingStreamOpen {
        private final Runnable opener;
        private final Consumer<IOException> onFail;

        public PendingStreamOpen(Runnable opener, Consumer<IOException> onFail) {
            this.opener = opener;
            this.onFail = onFail;
        }

        public void run() {
            opener.run();
        }

        public void fail(IOException cause) {
            onFail.accept(cause);
        }
    }

    private int activeStreams;
    private int maxConcurrentStreams = Integer.MAX_VALUE;
    private boolean draining;
    private int lastGoAwayStreamId = Integer.MAX_VALUE;
    private final Queue<PendingStreamOpen> pendingOpeners = new ArrayDeque<>();
    private volatile Object partitionKey;

    public synchronized boolean tryAcquireStream() {
        if (draining) {
            return false;
        }
        if (activeStreams >= maxConcurrentStreams) {
            return false;
        }
        activeStreams++;
        return true;
    }

    public synchronized void releaseStream() {
        activeStreams--;
        drainPending();
    }

    public synchronized void addPendingOpener(PendingStreamOpen entry) {
        if (draining) {
            entry.fail(new IOException("HTTP/2 connection is draining (GOAWAY received)"));
            return;
        }
        pendingOpeners.add(entry);
        drainPending();
    }

    /**
     * Runs as many pending openers as stream slots allow.
     * Must be called while holding the lock.
     */
    private void drainPending() {
        while (!pendingOpeners.isEmpty() && !draining && activeStreams < maxConcurrentStreams) {
            PendingStreamOpen entry = pendingOpeners.poll();
            activeStreams++;
            entry.run();
        }
    }

    public synchronized void updateMaxConcurrentStreams(int maxConcurrentStreams) {
        this.maxConcurrentStreams = maxConcurrentStreams;
        drainPending();
    }

    public synchronized int getMaxConcurrentStreams() {
        return maxConcurrentStreams;
    }

    public synchronized int getActiveStreams() {
        return activeStreams;
    }

    public synchronized boolean isDraining() {
        return draining;
    }

    public synchronized void setDraining(int lastStreamId) {
        this.lastGoAwayStreamId = lastStreamId;
        this.draining = true;
    }

    /**
     * Marks the connection as draining and returns all pending openers that were
     * waiting for stream slots. The caller must fail each returned entry.
     */
    public synchronized List<PendingStreamOpen> setDrainingAndDrainPending(int lastStreamId) {
        this.lastGoAwayStreamId = lastStreamId;
        this.draining = true;
        List<PendingStreamOpen> drained = new ArrayList<>(pendingOpeners);
        pendingOpeners.clear();
        return drained;
    }

    public synchronized int getLastGoAwayStreamId() {
        return lastGoAwayStreamId;
    }

    public void setPartitionKey(Object partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Object getPartitionKey() {
        return partitionKey;
    }
}
