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

import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.entity.PartnerRecommender;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CachePartnerRecommender extends BasicReadOnlyCache<PartnerRecommender> {
    protected CachePartnerRecommender2 cacheById;

    public CachePartnerRecommender(final JdbcDataProvider provider) {
        super(provider, "select id, partner_id, recommender_id, name from partner_recommender");
        cacheById = new CachePartnerRecommender2((PostgreSqlDataProvider) provider);
    }

    @Override
    public void refresh() {
        super.refresh();
        cacheById.refresh();
    }

    @Override
    protected PartnerRecommender createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException {
        return new PartnerRecommender(
            resultSet.getLong(1),
            link.getPartnerManager().getById(resultSet.getLong(2)),
            link.getRecommenderManager().getById(resultSet.getLong(3)),
            resultSet.getString(4)
        );
    }

    @Override
    protected String keyFromEntity(final PartnerRecommender entity) {
        return entity.getPartner().getId() + "|" + entity.getName();
    }

    public PartnerRecommender getByPartnerAndName(final Partner partner, final String name) {
        return byEntity(new PartnerRecommender(null, partner, null, name));
    }

    public PartnerRecommender getById(final Long id) {
        return cacheById.byId(id);
    }
}


// also cache by id
class CachePartnerRecommender2 extends SimpleReadOnlyCache<PartnerRecommender> {
    public CachePartnerRecommender2(final PostgreSqlDataProvider provider) {
        super(provider, "select id, partner_id, recommender_id, name from partner_recommender", 1, 4);
    }

    @Override
    protected PartnerRecommender createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException {
        return new PartnerRecommender(
                resultSet.getLong(1),
                link.getPartnerManager().getById(resultSet.getLong(2)),
                link.getRecommenderManager().getById(resultSet.getLong(3)),
                resultSet.getString(4)
        );
    }
}
