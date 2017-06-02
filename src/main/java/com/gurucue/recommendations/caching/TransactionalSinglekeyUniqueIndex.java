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

public final class TransactionalSinglekeyUniqueIndex<K, V extends Id<?>> extends TransactionalUniqueIndex<K, V> {
    protected final IndexKeyFactory<K, V> keyFactory;

    protected TransactionalSinglekeyUniqueIndex(final ConcurrentMap<K, TransactionalEntity<V>> index, final IndexKeyFactory<K, V> keyFactory) {
        super(index);
        this.keyFactory = keyFactory;
    }

    @Override
    public void bulkFill(final Collection<TransactionalEntity<V>> entities) {
        for (final TransactionalEntity<V> entity : entities) {
            if (entity == null) continue;
            final V value = entity.getCurrentValue();
            if (value == null) continue;
            final K key = keyFactory.keyOf(value);
            if (key == null) continue;
            index.put(key, entity);
        }
    }

    @Override
    public void changed(final V previousValue, final TransactionalEntity<V> currentEntity) {
        final K previousKey = previousValue == null ? null : keyFactory.keyOf(previousValue);
        final K currentKey = (currentEntity == null) || (currentEntity.getCurrentValue() == null) ? null : keyFactory.keyOf(currentEntity.getCurrentValue());
        if (currentKey == null) {
            if (previousKey == null) return;
            // remove the previous value
            index.remove(previousKey);
        }
        else if (previousKey == null) {
            index.put(currentKey, currentEntity);
        }
        else {
            if (!previousKey.equals(currentKey)) index.remove(previousKey);
            index.put(currentKey, currentEntity);
        }
    }

    public static <K, V extends Id<?>> TransactionalSinglekeyUniqueIndex<K, V> newHashIndex(final IndexKeyFactory<K, V> keyFactory) {
        return new TransactionalSinglekeyUniqueIndex<>(new ConcurrentHashMap<K, TransactionalEntity<V>>(), keyFactory);
    }

    public static <K, V extends Id<?>> TransactionalSinglekeyUniqueIndex<K, V> newOrderedIndex(final IndexKeyFactory<K, V> keyFactory, final Comparator<K> comparator) {
        return new TransactionalSinglekeyUniqueIndex<>(new ConcurrentSkipListMap<K, TransactionalEntity<V>>(comparator), keyFactory);
    }
}
