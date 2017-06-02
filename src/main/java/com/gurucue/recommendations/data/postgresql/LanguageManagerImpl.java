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

import com.gurucue.recommendations.entity.Language;
import com.gurucue.recommendations.entitymanager.LanguageManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Implementation of the Language Manager interface for PostgreSQL.
 */
class LanguageManagerImpl implements LanguageManager {
    private static final Logger log = LogManager.getLogger(LanguageManagerImpl.class);

    private final PostgreSqlDataLink dataLink;

    LanguageManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public Language getById(final Long id) {
        return dataLink.provider.languageCache.byId(id);
    }

    @Override
    public Language getByIdentifier(final String identifier) {
        return dataLink.provider.languageCache.byIdentifier(identifier);
    }

    @Override
    public List<Language> list() {
        return dataLink.provider.languageCache.list();
    }
}
