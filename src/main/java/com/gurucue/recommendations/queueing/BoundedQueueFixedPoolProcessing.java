/*
 * This file is part of Guru Cue Search & Recommendation Engine.
 * Copyright (C) 2017 Guru Cue Ltd.
 *
 * Guru Cue Search & Recommendation Engine is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * Guru Cue Search & Recommendation Engine is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Guru Cue Search & Recommendation Engine. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.gurucue.recommendations.queueing;

import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.TimerListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class BoundedQueueFixedPoolProcessing<T extends Payload> implements TimerListener {
    private static final Logger log = LogManager.getLogger(BoundedQueueFixedPoolProcessing.class);
    private final Lock lock = new ReentrantLock();
    private final Condition newElementInQueue = lock.newCondition();
    private final Condition elementTakenFromQueue = lock.newCondition();
    private final ProcessorFactory<T> processorFactory;
    private final LinkedList<T> payloadQueue;
    private final String threadNamePrefix;
    private int submitCounter = 0;
    private int takeCounter = 0;
    private int queueLimit;
    private int currentQueueSize = 0;
    private ProcessingThread<T>[] threads;
    private volatile boolean isStopped;
    private volatile int previousQueueSize = 0;

    public BoundedQueueFixedPoolProcessing(final String queueName, final int threadPoolSize, final int queueSize, final ProcessorFactory<T> processorFactory) {
        isStopped = false;
        payloadQueue = new LinkedList<>();
        this.threadNamePrefix = queueName + " queue processor #";
        queueLimit = queueSize;
        threads = new ProcessingThread[threadPoolSize];
        this.processorFactory = processorFactory;
        for (int i = 0; i < threadPoolSize; i++) {
            threads[i] = new ProcessingThread<T>(this, processorFactory.createProcessor(), threadNamePrefix + i);
        }
        for (int i = 0; i < threadPoolSize; i++) threads[i].start();
        Timer.INSTANCE.schedule(Timer.currentTimeMillis() + 10000L, this);
    }

    public void submit(final T payload) throws InterruptedException {
        if (isStopped) throw new RejectedExecutionException("The processor has been stopped");
//        log.debug("[" + Thread.currentThread().getId() + "] Submitting payload: " + payload.description());
        lock.lock();
        try {
            while (currentQueueSize > queueLimit) {
                elementTakenFromQueue.await();
            }
            payloadQueue.offer(payload); // LinkedList always returns true
            currentQueueSize++;
            submitCounter++;
            newElementInQueue.signal();
        }
        finally {
            lock.unlock();
        }
    }

/*    public boolean submit(final T payload, final long timeoutMillis) {
        final long threadId = Thread.currentThread().getId();
        log.debug("[" + threadId + "] Submitting payload: " + payload.description());
        if (isStopped) throw new RejectedExecutionException("The processor has been stopped");
        final boolean success;
        readLock.lock();
        try {
            try {
                success = payloadQueue.offer(payload, timeoutMillis, TimeUnit.MILLISECONDS);
                if (success) newElementInQueue.signal();
            } catch (InterruptedException e) {
                log.debug("[" + threadId + "] Interrupted while attempting to submit: " + e.toString(), e);
                return false;
            }
        }
        finally {
            readLock.unlock();
        }
        if (success) log.debug("[" + threadId + "] Submitted OK");
        else log.debug("[" + threadId + "] Submission failed");
        return success;
    }*/

    public void shutdown() {
        log.debug("Shutting down");
        if (isStopped) return;
        isStopped = true;
        final ProcessingThread<T>[] t;
        final int n;

        lock.lock();
        try {
            n = threads.length;
            t = Arrays.copyOf(threads, n);
            for (int i = n - 1; i >= 0; i--) {
                payloadQueue.push((T) Payload.FINISH_MARKER);
            }
            newElementInQueue.signalAll();
        }
        finally {
            lock.unlock();
        }

        for (int i = n - 1; i >= 0; i--) {
            try {
//                log.debug("Stopping thread #" + i);
                t[i].stop();
//                log.debug("Thread #" + i + " stopped");
            }
            catch (Exception e) {
                log.error("Failed to stop a thread: " + e.toString(), e);
            }
        }
        log.debug("Shutdown complete: all threads stopped");
    }

    T take(final ProcessingThread processingThread) throws InterruptedException {
        T payload;
        lock.lock();
        try {
            if (processingThread.isGone) return (T) Payload.FINISH_MARKER;
            while ((payload = payloadQueue.poll()) == null) {
                newElementInQueue.await();
                if (processingThread.isGone) return (T) Payload.FINISH_MARKER;
            }
            currentQueueSize--;
            takeCounter++;
            elementTakenFromQueue.signal();
        }
        finally {
            lock.unlock();
        }
//        log.debug("[" + Thread.currentThread().getId() + "] Dequeued: " + payload.description());
        return payload;
    }

    public void resizeQueue(final int newSize) {
        lock.lock();
        try {
            if (newSize < (threads.length * 10)) throw new IllegalArgumentException("The queue must be at least so big to accommodate 10 elements per worker thread, which is currently " + (threads.length * 10));
            log.info("Resizing queue capacity from " + queueLimit + " to " + newSize);
            queueLimit = newSize;
        }
        finally {
            lock.unlock();
        }
        log.info("Queue successfully resized to " + newSize);
    }

    public void resizeThreadPoolSize(final int newSize) {
        if (newSize < 1) throw new IllegalArgumentException("Cannot resize to less than 1 thread");
        final ProcessingThread<T>[] removedThreads;
        final int delta;
        // TODO: use a separate lock for resizing thread pool
        lock.lock();
        try {
            if (newSize == threads.length) return;
            log.info("Resizing thread pool from " + threads.length + " to " + newSize);
            final ProcessingThread<T>[] newThreads = new ProcessingThread[newSize];
            if (newSize < threads.length) {
                // truncate thread pool
                delta = threads.length - newSize;
                removedThreads = new ProcessingThread[delta];
                int i = 0;
                while (i < newSize) {
                    newThreads[i] = threads[i];
                    i++;
                }
                while (i < threads.length) {
                    final ProcessingThread<T> t = threads[i];
                    t.isGone = true;
                    removedThreads[i - newSize] = t;
                    i++;
                }
            }
            else {
                // increase thread pool
                delta = newSize - threads.length;
                removedThreads = new ProcessingThread[0];
                int i = 0;
                while (i < threads.length) {
                    newThreads[i] = threads[i];
                    i++;
                }
                while (i < newSize) {
                    final ProcessingThread<T> t = new ProcessingThread<T>(this, processorFactory.createProcessor(), threadNamePrefix + i);
                    newThreads[i] = t;
                    t.start();
                    i++;
                }
            }
            threads = newThreads;

            if (removedThreads.length > 0) newElementInQueue.signalAll(); // wake every thread; only removed threads will terminate
        }
        finally {
            lock.unlock();
        }

        if (removedThreads.length > 0) {
            for (int i = removedThreads.length - 1; i >= 0; i--) {
                try {
                    removedThreads[i].stop();
                }
                catch (Exception e) {
                    log.error("Failed to stop a removed thread: " + e.toString(), e);
                }
            }
            log.info("Thread pool resize complete: " + delta + " threads removed");
        }
        else {
            log.info("Thread pool resize complete: " + delta + " threads added");
        }
    }

    @Override
    public void onTimerExpired(final long expiryTime) {
        final int l;
        final int submitted;
        final int taken;
        lock.lock();
        try {
            l = currentQueueSize;
            submitted = submitCounter;
            submitCounter = 0;
            taken = takeCounter;
            takeCounter = 0;
        }
        finally {
            lock.unlock();
        }
        // do not log, if the size is zero for a longer time
        if ((previousQueueSize != 0) || (l != 0) || (submitted > 0) || (taken > 0)) {
            log.debug("Current consumer event queue size: " + l + ", since last time submitted: " + submitted + ", processed: " + taken);
        }
        previousQueueSize = l;
        Timer.INSTANCE.schedule(Timer.currentTimeMillis() + 10000L, this); // every 10 seconds
    }
}
