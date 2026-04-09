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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class Http2ConnectionStateTest {

    @Test
    void tryAcquireStream_respectsMaxConcurrentStreams() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(2);

        assertTrue(state.tryAcquireStream());
        assertTrue(state.tryAcquireStream());
        assertFalse(state.tryAcquireStream());
        assertEquals(2, state.getActiveStreams());
    }

    @Test
    void tryAcquireStream_returnsFalseWhenDraining() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.setDraining(0);

        assertFalse(state.tryAcquireStream());
    }

    @Test
    void releaseStream_decrementsAndRunsPendingOpener() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(1);

        assertTrue(state.tryAcquireStream());

        AtomicInteger ran = new AtomicInteger();
        state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                ran::incrementAndGet,
                cause -> fail("Should not fail")
        ));
        assertEquals(0, ran.get());

        state.releaseStream();
        assertEquals(1, ran.get());
        // The pending opener acquired a stream, so activeStreams should be 1
        assertEquals(1, state.getActiveStreams());
    }

    @Test
    void addPendingOpener_runsImmediatelyIfSlotAvailable() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(2);
        assertTrue(state.tryAcquireStream());

        AtomicInteger ran = new AtomicInteger();
        state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                ran::incrementAndGet,
                cause -> fail("Should not fail")
        ));
        assertEquals(1, ran.get());
        assertEquals(2, state.getActiveStreams());
    }

    @Test
    void addPendingOpener_failsImmediatelyWhenDraining() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.setDraining(0);

        AtomicReference<IOException> failCause = new AtomicReference<>();
        state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                () -> fail("Should not run"),
                failCause::set
        ));
        assertNotNull(failCause.get());
        assertTrue(failCause.get().getMessage().contains("draining"));
    }

    @Test
    void setDrainingAndDrainPending_returnsAndClearsPending() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(1);

        assertTrue(state.tryAcquireStream());

        AtomicInteger ran = new AtomicInteger();
        AtomicReference<IOException> failCause = new AtomicReference<>();

        state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                ran::incrementAndGet,
                failCause::set
        ));
        assertEquals(0, ran.get()); // couldn't run yet

        List<Http2ConnectionState.PendingStreamOpen> drained = state.setDrainingAndDrainPending(1);
        assertEquals(1, drained.size());

        // Fail the drained entries
        drained.forEach(p -> p.fail(new IOException("GOAWAY")));
        assertNotNull(failCause.get());

        // No more pending after drain
        assertEquals(0, state.setDrainingAndDrainPending(1).size());
    }

    @Test
    void concurrentAcquireRelease_noStreamLeaks() throws Exception {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(10);

        int threadCount = 20;
        int iterations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int i = 0; i < iterations; i++) {
                        if (state.tryAcquireStream()) {
                            // Simulate some work
                            Thread.yield();
                            state.releaseStream();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        assertEquals(0, errors.get());
        assertEquals(0, state.getActiveStreams(), "All streams should be released");
    }

    @Test
    void concurrentAddPendingAndRelease_noPendingLost() throws Exception {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(1);

        int iterations = 500;
        AtomicInteger completedCount = new AtomicInteger();
        CountDownLatch allDone = new CountDownLatch(iterations);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        // Acquire the single slot
        assertTrue(state.tryAcquireStream());

        // Submit many pending openers
        for (int i = 0; i < iterations; i++) {
            state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                    () -> {
                        completedCount.incrementAndGet();
                        // Release immediately to let the next one run
                        state.releaseStream();
                        allDone.countDown();
                    },
                    cause -> {
                        allDone.countDown();
                    }
            ));
        }

        // Release the initial slot to trigger the chain
        state.releaseStream();

        assertTrue(allDone.await(10, TimeUnit.SECONDS), "All pending openers should have been processed");
        assertEquals(iterations, completedCount.get(), "All pending openers should have run");
        assertEquals(0, state.getActiveStreams());

        executor.shutdown();
    }

    @Test
    void updateMaxConcurrentStreams_drainsPending() {
        Http2ConnectionState state = new Http2ConnectionState();
        state.updateMaxConcurrentStreams(1);

        assertTrue(state.tryAcquireStream());

        AtomicInteger ran = new AtomicInteger();
        state.addPendingOpener(new Http2ConnectionState.PendingStreamOpen(
                ran::incrementAndGet,
                cause -> fail("Should not fail")
        ));
        assertEquals(0, ran.get());

        // Increase max concurrent streams to allow the pending opener
        state.updateMaxConcurrentStreams(2);
        assertEquals(1, ran.get());
        assertEquals(2, state.getActiveStreams());
    }
}
