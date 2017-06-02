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

import com.gurucue.recommendations.entity.Consumer;
import com.gurucue.recommendations.entity.PartnerRecommender;
import com.gurucue.recommendations.entity.RecommenderConsumerOverride;
import com.gurucue.recommendations.entitymanager.RecommenderConsumerOverrideManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Implementation of the Partner Manager interface for PostgreSQL.
 */
class RecommenderConsumerOverrideManagerImpl implements RecommenderConsumerOverrideManager {
    private static final Logger log = LogManager.getLogger(RecommenderConsumerOverrideManagerImpl.class);

    private final PostgreSqlDataLink dataLink;

    RecommenderConsumerOverrideManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public RecommenderConsumerOverride getByConsumerAndOriginalPartnerRecommender(Consumer consumer, PartnerRecommender partnerRecommender) {
        return dataLink.provider.recommenderConsumerOverrideCache.getByConsumerAndOriginalPartnerRecommender(consumer, partnerRecommender);
    }
}
