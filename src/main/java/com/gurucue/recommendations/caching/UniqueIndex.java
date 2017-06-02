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
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class UniqueIndex<K, V extends Id<?>> implements Index<K, V> {
    protected final ConcurrentMap<K, V> index;

    protected UniqueIndex(final ConcurrentMap<K, V> index) {
        this.index = index;
    }

    public V find(final K key) {
        return index.get(key);
    }

    @Override
    public void replaceAll(final Collection<V> entities) {
        index.clear();
        for (final V entity : entities) changed(null, entity);
    }

    @Override
    public void internalRemove(final K key, final V entity) {
        index.remove(key, entity);
    }

    @Override
    public void internalAdd(final K key, final V entity) {
        index.put(key, entity);
    }

    @Override
    public void internalReplace(final K key, final V before, final V after) {
        index.put(key, after);
    }

    @Override
    public void destroy() {
        index.clear();
    }

    public Map<K, V> snapshot() {
        return new HashMap<>(index);
    }

    protected static class UnikeyIndex<K, V extends Id<?>> extends UniqueIndex<K, V> {
        private final UnikeyIndexDefinition<K, V> definition;

        UnikeyIndex(final ConcurrentMap<K, V> index, final UnikeyIndexDefinition<K, V> definition) {
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

    protected static class MultikeyIndex<K, V extends Id<?>> extends UniqueIndex<K, V> {
        private final MultikeyIndexDefinition<K, V> definition;

        MultikeyIndex(final ConcurrentMap<K, V> index, final MultikeyIndexDefinition<K, V> definition) {
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

    public static <K, V extends Id<?>> UniqueIndex<K, V> newHashIndex(final UnikeyIndexDefinition<K, V> definition) {
        return new UnikeyIndex<>(new ConcurrentHashMap<K, V>(), definition);
    }

    public static <K, V extends Id<?>> UniqueIndex<K, V> newHashIndex(final MultikeyIndexDefinition<K, V> definition) {
        return new MultikeyIndex<>(new ConcurrentHashMap<K, V>(), definition);
    }

    public static <K, V extends Id<?>> UniqueIndex<K, V> newOrderedIndex(final UnikeyIndexDefinition<K, V> definition, final Comparator<K> comparator) {
        return new UnikeyIndex<>(new ConcurrentSkipListMap<K, V>(comparator), definition);
    }

    public static <K, V extends Id<?>> UniqueIndex<K, V> newOrderedIndex(final MultikeyIndexDefinition<K, V> definition, final Comparator<K> comparator) {
        return new MultikeyIndex<>(new ConcurrentSkipListMap<K, V>(comparator), definition);
    }
}
