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

import com.gurucue.recommendations.queueing.QueueWorker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerAnonymizer implements QueueWorker<Long> {
    private static final Logger log = LogManager.getLogger(ConsumerAnonymizer.class);
    private static final int retries = 10;

    private final PostgreSqlDataProvider provider;
    private PostgreSqlDataLink link;

    ConsumerAnonymizer(final PostgreSqlDataProvider provider) {
        this.provider = provider;
        this.link = null;
    }

    @Override
    public void process(final Long consumerId) {
        if (consumerId == null) {
            log.error("Attempted to anonymize a null consumer ID");
            return;
        }
        for (int i = 1; i <= retries; i++) {
            try {
                if (link == null) {
                    link = provider.newJdbcDataLink();
                }
                link.getConsumerManager().anonymize(consumerId);
                link.commit();
                return;
            }
            catch (Exception e) {
                log.error("Exception while anonymizing consumer " + consumerId + "(try " + i + "/" + retries + "): " + e.toString(), e);
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

        log.error("IMPORTANT! Failed to anonymize consumer " + consumerId + ", administrative intervention required");
    }
}
