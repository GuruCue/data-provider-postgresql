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
package com.gurucue.recommendations.caching;

import com.gurucue.recommendations.Id;
import com.gurucue.recommendations.dto.ConsumerEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ConsumerCacheEntity implements Entity, Id<Long> {
    private static final Logger log = LogManager.getLogger(ConsumerCacheEntity.class);
    final ConsumerEntity consumer;

    protected ConsumerCacheEntity(final ConsumerEntity consumer) {
        this.consumer = consumer;
    }

    @Override
    public Long getId() {
        return consumer.id;
    }

    @Override
    public Entity duplicate() {
        return new ConsumerCacheEntity(consumer);
    }

    public ConsumerCacheEntity delete(final ConsumerCache cache) {
        log.debug("Removing consumer " + consumer.id + " from cache");
        return cache.cache.remove(consumer.id);
    }

    @Override
    public void destroy() {
        // nothing
    }

    public ConsumerCacheEntity save(final ConsumerCache cache) {
        return cache.cache.put(consumer.id, this);
    }

    public static ConsumerCacheEntity create(
            final ConsumerEntity consumer
    ) {
        return new ConsumerCacheEntity(consumer);
    }
}
