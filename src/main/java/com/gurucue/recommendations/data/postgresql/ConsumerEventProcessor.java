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

import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.queueing.Processor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ConsumerEventProcessor implements Processor<ConsumerEventPayload> {
    private static final Logger log = LogManager.getLogger(ConsumerEventProcessor.class);
    private static final Logger errorLogConsumerEvent = LogManager.getLogger("errors.db.ConsumerEvent");
    private final PostgreSqlDataProvider provider;
    private JdbcDataLink link;

    ConsumerEventProcessor(final PostgreSqlDataProvider provider) {
        this.provider = provider;
        this.link = null;
    }

    @Override
    public void consume(final ConsumerEventPayload payload) {
        for (int i = 0; i < 10; i++) {
            try {
                if (link == null) {
                    link = provider.newJdbcDataLink();
                }
                link.getConsumerEventManager().save(payload.consumerEvent);
                link.commit();
                return;
            }
            catch (Exception e) {
                log.error("Exception while saving an event: " + e.toString(), e);
                try {
                    link.rollback();
                } catch (Exception e1) {
                }
                link.close();
                link = null;
            }

            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                log.error("Interrupted while sleeping after an exception: " + e.toString(), e);
            }
        }

        log.error("Failed to save an event, dumping content to logs");
        if (payload.consumerEvent != null) {
            final StringBuilder consumerEventBuilder = new StringBuilder(1024);
            payload.consumerEvent.toPgTsv(consumerEventBuilder);
            errorLogConsumerEvent.error(consumerEventBuilder.toString());
        }
    }

    @Override
    public void close() {
        if (link != null) {
            link.close();
            link = null;
        }
    }

    @Override
    public long getMaxIdleMillis() {
        return 50L;
    }

    @Override
    public void idle() {

    }
}
