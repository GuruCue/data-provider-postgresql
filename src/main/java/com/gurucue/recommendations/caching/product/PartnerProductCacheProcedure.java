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
package com.gurucue.recommendations.caching.product;

import com.gurucue.recommendations.caching.PartnerProductCache;

/**
 * Represents a procedure to be invoked with a PartnerProductCache
 * instance, usually used with iteration over a collection.
 */
public interface PartnerProductCacheProcedure {
    /**
     * A processing method using the given PartnerProductCache.
     * To continue with the next invocation <code>true</code>
     * should be returned, or <code>false</code> to stop
     * iterating.
     *
     * @param cache the cache instance to use
     * @return whether to continue with the next iteration
     */
    boolean execute(PartnerProductCache cache);
}
