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

import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class QueueProcessor<T> {

    private static final class ThreadRunner<T> implements Runnable, TimerListener {
        private final Logger log;
        private final String name;

        // queueing stuff
        private int queueLimit = 100000;
        private final Lock lock = new ReentrantLock();
        private final Condition itemAdded = lock.newCondition();
        private final Condition itemRemoved = lock.newCondition();
        private final LinkedList<T> queue = new LinkedList<>();
        private int queueSize;
        private int submitted;
        private int consumed;
        private boolean running = false;

        // processing stuff
        private final QueueWorkerFactory<T> workerFactory;
        private QueueWorker[] workers;
        private Thread[] threads;
        private int poolSize;
        private int activeThreadCount;
        private int waitingSubmits;
        private long timerMillis;

        ThreadRunner(final String name, final QueueWorkerFactory<T> workerFactory, final int initialPoolSize) {
            log = LogManager.getLogger(QueueProcessor.class.getCanonicalName() + "." + name);
            this.name = name;
            this.workerFactory = workerFactory;
            this.poolSize = initialPoolSize;
        }

        final void start() {
            lock.lock();
            try {
                if (running) throw new IllegalStateException("Worker threads already running");
                queue.clear();
                queueSize = 0;
                submitted = 0;
                consumed = 0;
                activeThreadCount = 0;
                waitingSubmits = 0;
                running = true;
                workers = new QueueWorker[poolSize];
                threads = new Thread[poolSize];
                for (int i = threads.length - 1; i >= 0; i--) {
                    workers[i] = workerFactory.createWorker(i);
                    threads[i] = new Thread(this, "QueueProcessor-" + name + "-" + i);
                }
                for (int i = threads.length - 1; i >= 0; i--) threads[i].start();
            }
            finally {
                lock.unlock();
            }
            synchronized (this) {
                timerMillis = ((Timer.currentTimeMillis() + 10000L) / 10000L) * 10000L; // every 10 seconds, rounded to the first 1/6th of a minute
                Timer.INSTANCE.schedule(timerMillis, this);
            }
        }

        final void stop() {
            synchronized (this) {
                Timer.INSTANCE.unschedule(timerMillis, this);
            }
            lock.lock();
            try {
                if (!running) return;
                running = false;
                while (waitingSubmits != 0) {
                    try {
                        itemAdded.await();
                    }
                    catch (InterruptedException e) {
                        log.warn("Interrupted while waiting for threads waiting to enqueue to disappear");
                        break;
                    }
                }
                itemAdded.signalAll();
            }
            finally {
                lock.unlock();
            }
            for (int i = threads.length - 1; i >= 0; i--) {
                try {
                    threads[i].join();
                    threads[i] = null;
                }
                catch (InterruptedException e) {
                    log.warn("Interrupted while waiting to join the worker thread #" + i + ": " + e.toString(), e);
                }
            }
        }

        final void submit(final T item) {
            lock.lock();
            try {
                if (!running) throw new IllegalStateException("Cannot queue an item: worker threads are not running");
                waitingSubmits++;
                try {
                    while (queueSize >= queueLimit) {
                        try {
                            itemRemoved.await();
                        } catch (InterruptedException e) {
                            log.warn("Interrupted while waiting for a free queue slot: " + e.toString(), e);
                        }
                    }
                }
                finally {
                    waitingSubmits--;
                }
                queue.add(item);
                queueSize++;
                submitted++;
                itemAdded.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        public final void run() {
            final long threadId = Thread.currentThread().getId();
            final int index;
            final QueueWorker<T> worker;
            lock.lock();
            try {
                index = activeThreadCount++;
                worker = workers[index];
            }
            finally {
                lock.unlock();
            }
            final String logPrefix = "[" + index + "/" + threadId + "] ";
            log.info(logPrefix + "Worker thread started");

            try {
                loop:
                for (;;) {
                    final T item;
                    try {
                        lock.lock();
                        try {
                            while (queueSize == 0) {
                                if (!running && (waitingSubmits == 0)) break loop;
                                try {
                                    itemAdded.await();
                                }
                                catch (InterruptedException e) {
                                    log.warn(logPrefix + "Interrupted while waiting for an item from the queue: " + e.toString(), e);
                                }
                            }
                            item = queue.poll();
                            queueSize--;
                            consumed++;
                            itemRemoved.signal();
                        }
                        finally {
                            lock.unlock();
                        }

                        worker.process(item);
                    }
                    catch (Throwable e) {
                        log.error(logPrefix + "Failed to process an item: " + e.toString(), e);
                    }
                }
            }
            finally {
                lock.lock();
                try {
                    activeThreadCount--;
                } finally {
                    lock.unlock();
                }
                log.info(logPrefix + "Worker thread exiting");
            }
        }

        @Override
        public final void onTimerExpired(final long expiryTime) {
            if (!running) return; // TODO: this is not synchronized
            final int size;
            final int c;
            final int s;
            final int a;
            final int w;
            lock.lock();
            try {
                size = queueSize;
                a = activeThreadCount;
                w = waitingSubmits;
                c = consumed;
                s = submitted;
                consumed = 0;
                submitted = 0;
            }
            finally {
                lock.unlock();
            }
            if ((c > 0) || (s > 0)) {
                log.info(name + " queue size/capacity: " + size + "/" + queueLimit + ", submitted since last time: " + s + ", processed: " + c + ", active threads: " + a + ", requests waiting to queue: " + w);
            }
            // rearm the timer
            synchronized (this) {
                timerMillis = ((Timer.currentTimeMillis() + 10000L) / 10000L) * 10000L; // next first 1/6th of a minute (= every 10 seconds on condition this processing takes 0 time)
                Timer.INSTANCE.schedule(timerMillis, this);
            }
        }
    }

    private final ThreadRunner<T> runner;

    public QueueProcessor(final String name, final QueueWorkerFactory<T> workerFactory, final int initialPoolSize) {
        runner = new ThreadRunner<>(name, workerFactory, initialPoolSize);
    }

    public void start() {
        runner.start();
    }

    public void stop() {
        runner.stop();
    }

    public void submit(final T item) {
        runner.submit(item);
    }
}
