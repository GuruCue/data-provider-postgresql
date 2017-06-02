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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SimpleCache<K, V extends Id<K>> implements Cache<K, V> {
    private static final Logger log = LogManager.getLogger(SimpleCache.class);

    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<>(300000);
    private final ReentrantReadWriteLock writerLock = new ReentrantReadWriteLock();
    private final Lock writerReadLock = writerLock.readLock();
    private final Lock writerWriteLock = writerLock.writeLock();
    private Index<?, V>[] indexArray = new Index[0];

    public SimpleCache() {

    }

    @Override
    public V put(final K id, final V entity) {
        if (entity == null) throw new NullPointerException("Attempted to cache null");
        final V after = entity;
        final V before;
        final Index<?, V>[] indexArray;
        writerReadLock.lock();
        try {
            before = map.put(id, after);
            indexArray = this.indexArray;
        }
        finally {
            writerReadLock.unlock();
        }
        changed(before, after, indexArray);
        return before;
    }

    @Override
    public V putIfAbsent(K id, V entity) {
        if (entity == null) throw new NullPointerException("Attempted to cache null");
        final V after = entity;
        final V before;
        final Index<?, V>[] indexArray;
        writerReadLock.lock();
        try {
            before = map.putIfAbsent(id, after);
            indexArray = this.indexArray;
        }
        finally {
            writerReadLock.unlock();
        }
        if (before == null) {
            // only if before == null, was there a change
            changed(before, after, indexArray);
        }
        return before;
    }

    @Override
    public V get(final K id) {
        return map.get(id);
    }

    @Override
    public V remove(final K id) {
        final V before;
        final Index<?, V>[] indexArray;
        writerReadLock.lock();
        try {
            before = map.remove(id);
            indexArray = this.indexArray;
        }
        finally {
            writerReadLock.unlock();
        }
        if (before != null) changed(before, null, indexArray);
        return before;
    }

    private void changed(final V before, final V after, final Index<?, V>[] indexArray) {
        for (int i = indexArray.length - 1; i >= 0; i--) {
            try {
                indexArray[i].changed(before, after);
            }
            catch (Throwable t) {
                log.error("Index modification failed: " + t.toString(), t);
            }
        }
    }

    @Override
    public void addIndex(final Index<?, V> index) {
        if (index == null) return;
        writerWriteLock.lock();
        try {
            index.replaceAll(map.values());
            final int n = indexArray.length;
            indexArray = Arrays.copyOf(indexArray, n+1);
            indexArray[n] = index;
        }
        finally {
            writerWriteLock.unlock();
        }
    }

    @Override
    public void destroy() {
        final Index[] oldIndexArray;
        final List<V> oldValues;
        writerWriteLock.lock();
        try {
            oldIndexArray = indexArray;
            indexArray = new Index[0];
            oldValues = new ArrayList<>(map.values());
            map.clear();
        }
        finally {
            writerWriteLock.unlock();
        }
        for (final Index<?, V> index : oldIndexArray) {
            index.destroy();
        }
    }

    public Collection<V> values () {
        return map.values();
    }

    public int size() {
        return map.size();
    }
}
