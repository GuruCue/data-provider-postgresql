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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class CommonIndex<K, I, V extends Id<I>> implements Index<K, V> {
    final protected ConcurrentMap<K, IndexBucket<I, V>> index;

    protected CommonIndex(final ConcurrentMap<K, IndexBucket<I, V>> index) {
        this.index = index;
    }

    public Collection<V> find(final K key) {
        if (key == null) return Collections.emptyList();
        final IndexBucket bucket = index.get(key);
        if (bucket == null) {
            return Collections.emptyList();
        }
        bucket.lock.lock();
        try {
            if (bucket.wasDeleted) return Collections.emptyList();
            return bucket.map.values();
        }
        finally {
            bucket.lock.unlock();
        }
    }

    @Override
    public void replaceAll(final Collection<V> entities) {
        index.clear();
        for (final V entity : entities) changed(null, entity);
    }

    @Override
    public void internalRemove(final K key, final V entity) {
        final IndexBucket bucket = index.get(key);
        if (bucket == null) return; // no bucket, therefore already absent
        if (bucket.map.remove(entity.getId()) == null) return; // already absent
        bucket.lock.lock();
        try {
            if (bucket.wasDeleted) return;
            if (bucket.map.size() == 0) {
                bucket.wasDeleted = true;
                index.remove(key);
            }
        }
        finally {
            bucket.lock.unlock();
        }
    }

    @Override
    public void internalAdd(final K key, final V entity) {
        for (;;) {
            IndexBucket bucket = index.get(key);
            if (bucket == null) {
                bucket = index.putIfAbsent(key, new IndexBucket(entity));
                if (bucket == null) return;
            }
            bucket.lock.lock();
            try {
                if (bucket.wasDeleted) continue;
                bucket.map.put(entity.getId(), entity);
            }
            finally {
                bucket.lock.unlock();
            }
            break;
        }
    }

    @Override
    public void internalReplace(final K key, final V before, final V after) {
        for (;;) {
            IndexBucket bucket = index.get(key);
            if (bucket == null) {
                // no such bucket yet: the before entity does not exist
                bucket = index.putIfAbsent(key, new IndexBucket(after));
                if (bucket == null) return;
            }
            bucket.lock.lock();
            try {
                if (bucket.wasDeleted) continue;
                bucket.map.remove(before.getId());
                bucket.map.put(after.getId(), after);
            }
            finally {
                bucket.lock.unlock();
            }
            break;
        }
    }

    @Override
    public void destroy() {
        final List<IndexBucket<I, V>> buckets = new ArrayList<>(index.values());
        index.clear();
        for (final IndexBucket<I, V> bucket : buckets) {
            bucket.lock.lock();
            try {
                bucket.map.clear();
                bucket.wasDeleted = true;
            }
            finally {
                bucket.lock.unlock();
            }
        }
    }

    public Map<K, Collection<V>> snapshot() {
        final Map<K, Collection<V>> result = new HashMap<>(index.size());
        index.forEach((final K key, final IndexBucket<I, V> bucket) -> {
            result.put(key, bucket.currentValues());
        });
        return result;
    }

    // Implementations

    protected static class UnikeyIndex<K, I, V extends Id<I>> extends CommonIndex<K, I, V> {
        private final UnikeyIndexDefinition<K, V> definition;

        UnikeyIndex(final ConcurrentMap<K, IndexBucket<I, V>> index, final UnikeyIndexDefinition<K, V> definition) {
            super(index);
            this.definition = definition;
        }

        @Override
        public void changed(final V before, final V after) {
            definition.changed(this, before, after);
        }

        @Override
        public IndexDefinition getDefinition() {
            return definition;
        }
    }

    protected static class MultikeyIndex<K, I, V extends Id<I>> extends CommonIndex<K, I, V> {
        private final MultikeyIndexDefinition<K, V> definition;

        MultikeyIndex(final ConcurrentMap<K, IndexBucket<I, V>> index, final MultikeyIndexDefinition<K, V> definition) {
            super(index);
            this.definition = definition;
        }

        @Override
        public void changed(final V before, final V after) {
            definition.changed(this, before, after);
        }

        @Override
        public IndexDefinition getDefinition() {
            return definition;
        }
    }

    protected static interface OrderedCommonIndex<K, V extends Id<?>> extends OrderedIndex<K, V> {
        Collection<V> find(K key);
    }

    protected static class OrderedUnikeyIndex<K, I, V extends Id<I>> extends UnikeyIndex<K, I, V> implements OrderedCommonIndex<K, V> {
        final ConcurrentNavigableMap<K, IndexBucket<I, V>> navigableIndex;

        OrderedUnikeyIndex(final ConcurrentNavigableMap<K, IndexBucket<I, V>> index, final UnikeyIndexDefinition<K, V> definition) {
            super(index, definition);
            navigableIndex = index;
        }

        @Override
        public Collection<V> findInRange(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive) {
            final List<V> result = new ArrayList<>();
            navigableIndex.subMap(fromKey, fromInclusive, toKey, toInclusive).forEach((final K key, final IndexBucket<I, V> bucket) -> {
                bucket.lock.lock();
                try {
                    result.addAll(bucket.map.values());
                }
                finally {
                    bucket.lock.unlock();
                }
            });
            return result;
        }
    }

    protected static class OrderedMultikeyIndex<K, I, V extends Id<I>> extends MultikeyIndex<K, I, V> implements OrderedCommonIndex<K, V> {
        final ConcurrentNavigableMap<K, IndexBucket<I, V>> navigableIndex;

        OrderedMultikeyIndex(final ConcurrentNavigableMap<K, IndexBucket<I, V>> index, final MultikeyIndexDefinition<K, V> definition) {
            super(index, definition);
            navigableIndex = index;
        }

        @Override
        public Collection<V> findInRange(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive) {
            final List<V> result = new ArrayList<>();
            navigableIndex.subMap(fromKey, fromInclusive, toKey, toInclusive).forEach((final K key, final IndexBucket<I, V> bucket) -> {
                bucket.lock.lock();
                try {
                    result.addAll(bucket.map.values());
                }
                finally {
                    bucket.lock.unlock();
                }
            });
            return result;
        }
    }

    // Factory methods

    public static <K, I, V extends Id<I>> CommonIndex<K, I, V> newHashIndex(final UnikeyIndexDefinition<K, V> definition) {
        return new UnikeyIndex<>(new ConcurrentHashMap<K, IndexBucket<I, V>>(), definition);
    }

    public static <K, I, V extends Id<I>> CommonIndex<K, I, V> newHashIndex(final MultikeyIndexDefinition<K, V> definition) {
        return new MultikeyIndex<>(new ConcurrentHashMap<K, IndexBucket<I, V>>(), definition);
    }

    public static <K, I, V extends Id<I>> OrderedCommonIndex<K, V> newOrderedIndex(final UnikeyIndexDefinition<K, V> definition, final Comparator<K> comparator) {
        return new OrderedUnikeyIndex<>(new ConcurrentSkipListMap<K, IndexBucket<I, V>>(comparator), definition);
    }

    public static <K, I, V extends Id<I>> OrderedCommonIndex<K, V> newOrderedIndex(final MultikeyIndexDefinition<K, V> definition, final Comparator<K> comparator) {
        return new OrderedMultikeyIndex<>(new ConcurrentSkipListMap<K, IndexBucket<I, V>>(comparator), definition);
    }
}
