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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class ProcessingThread<T extends Payload> {
    private static final Logger log = LogManager.getLogger(ProcessingThread.class);
    private final Processor<T> processor;
    private final String threadName;
    private boolean isRunning = false;
    private Thread thread = null;
    boolean isGone = false;

    ProcessingThread(final BoundedQueueFixedPoolProcessing<T> source, final com.gurucue.recommendations.queueing.Processor<T> consumer, final String threadName) {
        processor = new Processor(source, consumer, this);
        this.threadName = threadName;
    }

    synchronized void start() {
        if (isRunning) throw new IllegalStateException("The thread is already running");
        thread = new Thread(processor, threadName);
        thread.start();
        isRunning = true;
    }

    synchronized void stop() {
        if (!isRunning) return;
        final Thread t = thread;
        try {
            if (t.isAlive()) thread.join(500L);
            if (t.isAlive()) log.warn("[" + t.getId() + "] Thread failed to stop in 500 ms");
            isRunning = false;
            thread = null;
        } catch (InterruptedException e) {
            log.error("[" + t.getId() + "] Interrupted while waiting for the thread to stop: " + e.toString(), e);
        }
    }

    private static final class Processor<T extends Payload> implements Runnable {
        private static final Logger log = LogManager.getLogger(Processor.class);
        private final BoundedQueueFixedPoolProcessing<T> source;
        private final com.gurucue.recommendations.queueing.Processor<T> processor;
        private final ProcessingThread<T> owner;

        Processor(final BoundedQueueFixedPoolProcessing<T> source, final com.gurucue.recommendations.queueing.Processor<T> processor, final ProcessingThread<T> owner) {
            this.source = source;
            this.processor = processor;
            this.owner = owner;
        }

        @Override
        public void run() {
            final long threadId = Thread.currentThread().getId();
            final String logPrefix = "[" + threadId + "] ";
            // arbitrary startup delay
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                log.warn(logPrefix + "Interrupted during initial sleep: " + e.toString(), e);
            }

            log.debug(logPrefix + " Thread started");
            try {
                for (; ; ) {
                    try {
                        final T payload;
                        try {
                            payload = source.take(owner);
                        } catch (InterruptedException e) {
                            log.error(logPrefix + "Interrupted while trying to obtain a payload: " + e.toString(), e);
                            continue;
                        }
                        if (payload == Payload.FINISH_MARKER) return;
//                        log.debug(logPrefix + "Processing payload: " + payload.description());
                        processor.consume(payload);
                    }
                    catch (Throwable e) {
                        log.error(logPrefix + "Payload processing failed, sleeping for 500 ms: " + e.toString(), e);
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e1) {
                            log.warn(logPrefix + "Interrupted during after-exception sleep: " + e1.toString(), e1);
                        }
                    }
                }
            }
            finally {
                log.debug(logPrefix + "Thread exiting");
                processor.close();
            }
        }
    }
}
