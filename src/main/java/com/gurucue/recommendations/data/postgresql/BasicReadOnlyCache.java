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

import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * A cache that allows caching by arbitrary key derived from the entity being cached.
 */
public abstract class BasicReadOnlyCache<E> implements Cache {
    protected final Map<Object, E> mappingByKey = new HashMap<>();
    protected final JdbcDataProvider provider;
    protected final String selectSql;

    public BasicReadOnlyCache(final JdbcDataProvider provider, final String selectSql) {
        this.provider = provider;
        this.selectSql = selectSql;
    }

    @Override
    public void refresh() {
        mappingByKey.clear();
        try (final JdbcDataLink link = provider.newJdbcDataLink()) {
            try (final Statement statement = link.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                try (final ResultSet rs = statement.executeQuery(selectSql)) {
                    while (rs.next()) {
                        final E entity = createEntity(link, rs);
                        final Object key = keyFromEntity(entity);
                        mappingByKey.put(key, entity);
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("Error while refreshing cache: " + e.toString(), e);
            }
        }
    }

    public E byEntity(final E entity) {
        Object key = keyFromEntity(entity);
        return byKey(key);
    }

    protected E byKey(final Object key) {
        return mappingByKey.get(key);
    }

    protected abstract Object keyFromEntity(final E entity);

    protected abstract E createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException;


}
