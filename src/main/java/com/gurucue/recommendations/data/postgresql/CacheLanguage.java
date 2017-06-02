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
import com.gurucue.recommendations.entity.Language;
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

public class CacheLanguage implements Cache {
    static private final String selectSql = "select id, iso639_2t, iso639_1 from language";

    protected final TLongObjectMap<Language> mappingById = new TLongObjectHashMap<>();
    protected final Map<String, Language> mappingByIdentifier = new HashMap<>();
    protected final JdbcDataProvider provider;

    public CacheLanguage(final JdbcDataProvider provider) {
        this.provider = provider;
    }

    @Override
    public void refresh() {
        mappingById.clear();
        mappingByIdentifier.clear();
        try (final JdbcDataLink link = provider.newJdbcDataLink()) {
            try (final Statement statement = link.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                try (final ResultSet rs = statement.executeQuery(selectSql)) {
                    while (rs.next()) {
                        final long id = rs.getLong(1);
                        final String iso639_2t = rs.getString(2);
                        final String iso639_1 = rs.getString(3);
                        final Language entity = new Language(id, iso639_2t, iso639_1);
                        mappingById.put(id, entity);
                        mappingByIdentifier.put(iso639_2t, entity);
                        mappingByIdentifier.put(iso639_1, entity);
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("Error while refreshing cache: " + e.toString(), e);
            }
        }
    }

    public Language byId(final long id) {
        return mappingById.get(id);
    }

    public Language byIdentifier(final String identifier) {
        return mappingByIdentifier.get(identifier);
    }

    public List<Language> list() {
        return new ArrayList<>(mappingByIdentifier.values());
    }
}
