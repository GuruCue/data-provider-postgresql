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

import com.gurucue.recommendations.entity.DataType;
import com.gurucue.recommendations.entitymanager.DataTypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Implementation of the DataType Manager interface for PostgreSQL.
 */
class DataTypeManagerImpl implements DataTypeManager {
    private static final Logger log = LogManager.getLogger(DataTypeManagerImpl.class);

    private final PostgreSqlDataLink dataLink;

    DataTypeManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public DataType getById(final Long id) {
        return dataLink.provider.dataTypeCache.byId(id);
    }

    @Override
    public DataType getByIdentifier(final String identifier) {
        return dataLink.provider.dataTypeCache.byIdentifier(identifier);
    }

    @Override
    public List<DataType> list() {
        return dataLink.provider.dataTypeCache.list();
    }
}
