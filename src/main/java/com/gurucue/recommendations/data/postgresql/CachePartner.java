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
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CachePartner extends SimpleReadOnlyCache<Partner> {
    final CacheLanguage languageCache;

    public CachePartner(final PostgreSqlDataProvider provider) {
        super(provider, "select id, name, username, login_suffix, language_id from partner", 1, 3);
        this.languageCache = provider.languageCache;
    }

    @Override
    protected Partner createEntity(final JdbcDataLink link, final ResultSet resultSet) throws SQLException {
        String loginSuffix = resultSet.getString(4);
        if (resultSet.wasNull()) loginSuffix = null; // this is the only nullable field
        return new Partner(
                resultSet.getLong(1),
                resultSet.getString(2),
                resultSet.getString(3),
                loginSuffix,
                languageCache.byId(resultSet.getLong(5))
        );
    }
}
