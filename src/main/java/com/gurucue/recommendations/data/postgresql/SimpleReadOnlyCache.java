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
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SimpleReadOnlyCache<E> implements Cache {
    protected final TLongObjectMap<E> mappingById = new TLongObjectHashMap<>();
    protected final Map<String, E> mappingByIdentifier = new HashMap<>();
    protected final JdbcDataProvider provider;
    protected final String selectSql;
    protected final int idColumnIndex;
    protected final int identifierColumnIndex;

    public SimpleReadOnlyCache(final JdbcDataProvider provider, final String selectSql, final int idColumnIndex, final int identifierColumnIndex) {
        this.provider = provider;
        this.selectSql = selectSql;
        this.idColumnIndex = idColumnIndex;
        this.identifierColumnIndex = identifierColumnIndex;
    }

    @Override
    public void refresh() {
        // this works as long as there is only one thread mutating the cache at a time - I think
        mappingById.clear();
        mappingByIdentifier.clear();
        try (final JdbcDataLink link = provider.newJdbcDataLink()) {
            try (final Statement statement = link.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                try (final ResultSet rs = statement.executeQuery(selectSql)) {
                    while (rs.next()) {
                        final long id = rs.getLong(idColumnIndex);
                        final String identifier = rs.getString(identifierColumnIndex);
                        final E entity = createEntity(link, rs);
                        mappingById.put(id, entity);
                        mappingByIdentifier.put(identifier, entity);
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("Error while refreshing cache: " + e.toString(), e);
            }
        }
    }

    protected abstract E createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException;

    public E byId(final long id) {
        return mappingById.get(id);
    }

    public E byIdentifier(final String identifier) {
        return mappingByIdentifier.get(identifier);
    }

    public List<E> list() {
        return new ArrayList<E>(mappingByIdentifier.values());
    }
}
