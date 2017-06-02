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

/**
 * Wraps a DTO object, so it is suitable for caching.
 */
public interface Entity<K> {
    /**
     * Returns the unique entity ID used by the cache.
     * @return the unique entity ID in the cache
     */
    K getId();

    /**
     * Creates a duplicate (clone) of the entity.
     * @return an entity duplicate (clone)
     */
    @Deprecated
    Entity<K> duplicate();

    /**
     * Signals the entity that it was abruptly cut free (not a regular deletion).
     * This means it's not a part of any structure (such as cache or index)
     * anymore, so no bookkeeping of any kind is required, the entity should
     * only release any resources it is occupying.
     */
    void destroy();
}
