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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class MultikeyIndexDefinition<K, V extends Id<?>> implements IndexDefinition<K, V> {
    public abstract Collection<K> keys(V entity);

    @Override
    public void changed(final Index<K, V> index, final V before, final V after) {
        if (before == null) {
            if (after == null) return;
            for (final K key : keys(after)) {
                index.internalAdd(key, after);
            }
        }
        else if (after == null) {
            for (final K key : keys(before)) {
                index.internalRemove(key, before);
            }
        }
        else {
            final Set<K> beforeKeys = new HashSet<>(keys(before));
            for (final K key : keys(after)) {
                if (beforeKeys.remove(key)) index.internalReplace(key, before, after);
                else index.internalAdd(key, after);
            }
            for (final K key : beforeKeys) {
                index.internalRemove(key, before);
            }
        }
    }
}
