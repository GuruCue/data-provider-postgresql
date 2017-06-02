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

import com.gurucue.recommendations.dto.ConsumerEntity;
import com.gurucue.recommendations.entity.Consumer;
import com.gurucue.recommendations.entity.PartnerRecommender;
import com.gurucue.recommendations.entity.RecommenderConsumerOverride;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class CacheRecommenderConsumerOverride extends BasicReadOnlyCache<RecommenderConsumerOverride> {
    public CacheRecommenderConsumerOverride(final JdbcDataProvider provider) {
        super(provider, "select id, consumer_id, original_partner_recommender_id, override_partner_recommender_id from recommender_consumer_override");
    }

    @Override
    protected RecommenderConsumerOverride createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException {
        final ConsumerEntity consumerEntity = link.getConsumerManager().getById(resultSet.getLong(2));
        return new RecommenderConsumerOverride(
            resultSet.getLong(1),
            new Consumer(consumerEntity.id, consumerEntity.username, link.getPartnerManager().getById(consumerEntity.partnerId), new Timestamp(consumerEntity.activated)), // TODO: creating a Consumer instance is for backwards compatibility
            link.getPartnerRecommenderManager().getById(resultSet.getLong(3)),
            link.getPartnerRecommenderManager().getById(resultSet.getLong(4))
        );
    }

    @Override
    protected String keyFromEntity(final RecommenderConsumerOverride entity) {
        return entity.getConsumer().getId() + "|" + entity.getOriginalPartnerRecommender().getId();
    }

    public RecommenderConsumerOverride getByConsumerAndOriginalPartnerRecommender(final Consumer consumer, final PartnerRecommender partnerRecommender) {
        return byEntity(new RecommenderConsumerOverride(null, consumer, partnerRecommender, null));
    }
}
