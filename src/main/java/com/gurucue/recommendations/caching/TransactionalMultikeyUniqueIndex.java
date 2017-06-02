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
import com.gurucue.recommendations.TransactionalEntity;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class TransactionalMultikeyUniqueIndex<K, V extends Id<?>> extends TransactionalUniqueIndex<K, V> {
    protected final IndexKeysFactory<K, V> keysFactory;

    protected TransactionalMultikeyUniqueIndex(final ConcurrentMap<K, TransactionalEntity<V>> index, final IndexKeysFactory<K, V> keysFactory) {
        super(index);
        this.keysFactory = keysFactory;
    }

    @Override
    public void bulkFill(final Collection<TransactionalEntity<V>> entities) {
        for (final TransactionalEntity<V> entity : entities) {
            if (entity == null) continue;
            final V value = entity.getCurrentValue();
            if (value == null) continue;
            final Collection<? extends K> keys = keysFactory.keysOf(value);
            if ((keys == null) || (keys.isEmpty())) continue;
            for (final K key : keys) {
                if (key == null) continue;
                index.put(key, entity);
            }
        }
    }

    @Override
    public void changed(final V previousValue, final TransactionalEntity<V> currentEntity) {
        final Collection<? extends K> previousKeys = previousValue == null ? null : keysFactory.keysOf(previousValue);
        final Collection<? extends K> currentKeys = (currentEntity == null) || (currentEntity.getCurrentValue() == null) ? null : keysFactory.keysOf(currentEntity.getCurrentValue());
        if ((currentKeys == null) || (currentKeys.isEmpty())) {
            if ((previousKeys == null) || (previousKeys.isEmpty())) return;
            for (final K previousKey : previousKeys) index.remove(previousKey);
        }
        else if ((previousKeys == null) || (previousKeys.isEmpty())) {
            for (final K currentKey : currentKeys) index.put(currentKey, currentEntity);
        }
        else {
            final Set<K> previousLeftovers = new HashSet<>(previousKeys);
            for (final K currentKey : currentKeys) {
                index.put(currentKey, currentEntity);
                previousLeftovers.remove(currentKey);
            }
            for (final K previousKey : previousLeftovers) {
                index.remove(previousKey);
            }
        }
    }

    public static <K, V extends Id<?>> TransactionalMultikeyUniqueIndex<K, V> newHashIndex(final IndexKeysFactory<K, V> keysFactory) {
        return new TransactionalMultikeyUniqueIndex<>(new ConcurrentHashMap<K, TransactionalEntity<V>>(), keysFactory);
    }

    public static <K, V extends Id<?>> TransactionalMultikeyUniqueIndex<K, V> newOrderedIndex(final IndexKeysFactory<K, V> keysFactory, final Comparator<K> comparator) {
        return new TransactionalMultikeyUniqueIndex<>(new ConcurrentSkipListMap<K, TransactionalEntity<V>>(comparator), keysFactory);
    }
}
