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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public final class TransactionalMultikeyCommonIndex<K, V extends Id<?>> extends TransactionalCommonIndex<K, V> {
    protected final IndexKeysFactory<K, V> keysFactory;

    protected TransactionalMultikeyCommonIndex(ConcurrentMap<K, TransactionalIndexBucket<K, V, TransactionalEntity<V>>> index, final IndexKeysFactory<K, V> keysFactory) {
        super(index);
        this.keysFactory = keysFactory;
    }

    @Override
    public final void bulkFill(final Collection<TransactionalEntity<V>> transactionalEntities) {
        for (final TransactionalEntity<V> entity : transactionalEntities) {
            if (entity == null) continue;
            final V value = entity.getCurrentValue();
            if (value == null) continue;
            final Collection<? extends K> keys = keysFactory.keysOf(value);
            if ((keys == null) || (keys.isEmpty())) continue;
            for (final K key : keys) {
                if (key == null) continue;
                internalPut(key, entity);
            }
        }
    }

    @Override
    public final void changed(final V previousValue, final TransactionalEntity<V> currentEntity) {
        final Collection<? extends K> previousKeys = previousValue == null ? null : keysFactory.keysOf(previousValue);
        final Collection<? extends K> currentKeys = (currentEntity == null) || (currentEntity.getCurrentValue() == null) ? null : keysFactory.keysOf(currentEntity.getCurrentValue());
        if ((currentKeys == null) || (currentKeys.isEmpty())) {
            if ((previousKeys == null) || (previousKeys.isEmpty())) return;
            // deletion
            for (final K previousKey : previousKeys) internalRemove(previousKey, previousValue);
        }
        else if ((previousKeys == null) || (previousKeys.isEmpty())) {
            // addition
            for (final K currentKey : currentKeys) internalPut(currentKey, currentEntity);
        }
        else {
            // modification
            for (final K previousKey : previousKeys) {
                // remove all previous
                internalRemove(previousKey, previousValue);
            }
            for (final K currentKey : currentKeys) {
                // add all current
                internalPut(currentKey, currentEntity);
            }
        }
    }

    public static <K, V extends Id<?>> TransactionalMultikeyCommonIndex<K, V> newHashIndex(final IndexKeysFactory<K, V> keysFactory) {
        return new TransactionalMultikeyCommonIndex<>(new ConcurrentHashMap<K, TransactionalIndexBucket<K, V, TransactionalEntity<V>>>(), keysFactory);
    }

    public static <K, V extends Id<?>> TransactionalMultikeyCommonIndex<K, V> newOrderedIndex(final IndexKeysFactory<K, V> keysFactory, final Comparator<K> comparator) {
        return new TransactionalMultikeyCommonIndex<>(new ConcurrentSkipListMap<K, TransactionalIndexBucket<K, V, TransactionalEntity<V>>>(comparator), keysFactory);
    }
}
