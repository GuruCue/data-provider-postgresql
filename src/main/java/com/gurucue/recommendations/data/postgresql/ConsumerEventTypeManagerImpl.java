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

import com.gurucue.recommendations.entity.ConsumerEventType;
import com.gurucue.recommendations.entitymanager.ConsumerEventTypeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ConsumerEventTypeManagerImpl implements ConsumerEventTypeManager {
    private static final Logger log = LogManager.getLogger(ConsumerEventTypeManagerImpl.class);

    private final PostgreSqlDataLink dataLink;

    ConsumerEventTypeManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public ConsumerEventType getByIdentifier(final String identifier) {
        return dataLink.provider.consumerEventTypeCache.byIdentifier(identifier);
    }

    @Override
    public List<ConsumerEventType> list() {
        return dataLink.provider.consumerEventTypeCache.list();
    }
}
