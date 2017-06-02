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

import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CacheProductType extends SimpleReadOnlyCache<ProductType> {
    public CacheProductType(final JdbcDataProvider provider) {
        super(provider, "select id, identifier from product_type", 1, 2);
    }

    @Override
    protected ProductType createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException {
        return new ProductType(resultSet.getLong(1), resultSet.getString(2));
    }
}
