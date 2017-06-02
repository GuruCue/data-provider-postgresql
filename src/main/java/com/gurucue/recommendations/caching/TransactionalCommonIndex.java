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
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TransactionalCommonIndex<K, V extends Id<?>> implements TransactionalIndex<V> {
    protected final ConcurrentMap<K, TransactionalIndexBucket<K, V, TransactionalEntity<V>>> index;

    protected TransactionalCommonIndex(final ConcurrentMap<K, TransactionalIndexBucket<K, V, TransactionalEntity<V>>> index) {
        this.index = index;
    }

    public abstract void bulkFill(final Collection<TransactionalEntity<V>> entities);

    public abstract void changed(final V previousValue, final TransactionalEntity<V> currentEntity);

    /**
     * Returns the entities with the given key, or <code>null</code> if no such entity exists.
     * No locking is performed.
     *
     * @param key the key to use for lookup
     * @return a collection of entities with the given key
     */
    public final Collection<TransactionalEntity<V>> find(final K key) {
        final TransactionalIndexBucket<K, V, TransactionalEntity<V>> bucket = index.get(key);
        if (bucket == null) return Collections.emptyList();
        return bucket.values();
    }

    protected final TransactionalIndexBucket<K, V, TransactionalEntity<V>> findAndLockBucket(final K key, final boolean allowMissing) {
        // optimistic path
        final TransactionalIndexBucket<K, V, TransactionalEntity<V>> bucket = index.get(key);
        if (bucket != null) {
            bucket.lock();
            if (bucket.isDeleted) {
                // it got removed, forget it
                index.remove(key, bucket); // bug prevention: the bucket should already be removed by now, but we do it (defensively) anyway
                bucket.unlock();
                if (allowMissing) return null;
            }
            else {
                return bucket;
            }
        }
        else if (allowMissing) return null;
        // pessimistic path
        final TransactionalIndexBucket<K, V, TransactionalEntity<V>> newBucket = new TransactionalIndexBucket<>(key);
        newBucket.lock();
        for (;;) {
            final TransactionalIndexBucket<K, V, TransactionalEntity<V>> oldBucket = index.putIfAbsent(key, newBucket);
            if (oldBucket != null) {
                oldBucket.lock();
                if (oldBucket.isDeleted) {
                    index.remove(key, oldBucket); // bug prevention: the bucket should already be removed by now, but we do it (defensively) anyway
                    oldBucket.unlock();
                }
                else {
                    // bucket is mapped and alive
                    newBucket.unlock();
                    return oldBucket;
                }
            }
            else {
                return newBucket;
            }
        }
    }

    protected final void internalPut(final K key, final TransactionalEntity<V> entity) {
        final V value = entity.getCurrentValue();
        if (value == null) return; // TODO: log it
        final TransactionalIndexBucket<K, V, TransactionalEntity<V>> bucket = findAndLockBucket(key, false);
        try {
            bucket.put(value, entity);
        }
        finally {
            bucket.unlock();
        }
    }

    protected final void internalRemove(final K key, final V value) {
        final TransactionalIndexBucket<K, V, TransactionalEntity<V>> bucket = findAndLockBucket(key, true);
        if (bucket == null) return;
        try {
            bucket.remove(value);
            if (bucket.isEmpty()) {
                index.remove(key, bucket);
                bucket.isDeleted = true;
            }
        }
        finally {
            bucket.unlock();
        }
    }
}

final class TransactionalIndexBucket<K, V extends Id<?>, T extends TransactionalEntity<V>> extends ConcurrentHashMap<V, T> implements Id<K> {
    private final ReentrantLock lock = new ReentrantLock();
    private final K key;
    boolean isDeleted = false;

    TransactionalIndexBucket(final K key) {
        super();
        this.key = key;
    }

    @Override
    public K getId() {
        return key;
    }

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }
}