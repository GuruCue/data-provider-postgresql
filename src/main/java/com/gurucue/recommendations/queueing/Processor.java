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

public interface Processor<T extends Payload> {
    /**
     * Process the specified payload.
     *
     * @param payload the payload to process
     */
    void consume(T payload);

    /**
     * Free all resources, souch as database connection.
     */
    void close();

    /**
     * How long to wait for another payload before we decide we're idling.
     * If no payload has been obtained in the specified time, {@link #idle()}
     * will be invoked before we continue with waiting for a payload.
     *
     * @return milliseconds to wait for another payload, after that if no payload was received we invoke idle()
     */
    long getMaxIdleMillis();

    /**
     * Invoked to signal the consumer that we've waited for the maximum
     * time and no further payload has been received.
     */
    void idle();
}
