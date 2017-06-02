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
package com.gurucue.recommendations.data.postgresql;

import com.gurucue.recommendations.entity.ConsumerEvent;
import com.gurucue.recommendations.queueing.Payload;

public final class ConsumerEventPayload extends Payload {

    final ConsumerEvent consumerEvent;
    final String description;

    public ConsumerEventPayload(final ConsumerEvent consumerEvent) {
        this.consumerEvent = consumerEvent;

        final StringBuilder sb = new StringBuilder(256);
        sb.append("[ConsumerEventPayload: ");

        sb.append("log[responseCode=");
        sb.append(consumerEvent.getResponseCode());
        sb.append(", requestTimestamp=");
        if (consumerEvent.getRequestTimestamp() == null) sb.append("(null)");
        else sb.append(consumerEvent.getRequestTimestamp().getTime());
        sb.append(", requestDuration=");
        sb.append(consumerEvent.getRequestDuration());
        sb.append("], ");

        sb.append("event[type=");
        if (consumerEvent.getEventType() == null) sb.append("(null)");
        else sb.append(consumerEvent.getEventType().getIdentifier());
        sb.append(", consumer=[id=");
        if (consumerEvent.getConsumer() == null) sb.append("(null), username=(null)");
        else sb.append(consumerEvent.getConsumer().getId()).append(", username=").append(consumerEvent.getConsumer().getUsername());
        sb.append("], product=[id=");
        if (consumerEvent.getProduct() == null) sb.append("(null), code=(null)");
        else sb.append(consumerEvent.getProduct().getId()).append(", code=").append(consumerEvent.getProduct().partnerProductCode);
        sb.append("], eventTimestamp=");
        if (consumerEvent.getEventTimestamp() == null) sb.append("(null)");
        else sb.append(consumerEvent.getEventTimestamp().getTime());
        sb.append("]");

        sb.append("]");
        description = sb.toString();
    }

    @Override
    public String description() {
        return description;
    }
}
