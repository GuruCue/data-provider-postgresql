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

public abstract class UnikeyIndexDefinition<K, V extends Id<?>> implements IndexDefinition<K, V> {
    public abstract K key(V entity);

    @Override
    public void changed(final Index<K, V> index, final V before, final V after) {
        final K beforeKey = before == null ? null : key(before);
        final K afterKey = after == null ? null : key(after);
        if (beforeKey == null) {
            if (afterKey == null) return;
            index.internalAdd(afterKey, after);
        }
        else if (afterKey == null) {
            index.internalRemove(beforeKey, before);
        }
        else {
            if (beforeKey.equals(afterKey)) index.internalReplace(beforeKey, before, after);
            else {
                index.internalAdd(afterKey, after);
                index.internalRemove(beforeKey, before);
            }
        }
    }
}
