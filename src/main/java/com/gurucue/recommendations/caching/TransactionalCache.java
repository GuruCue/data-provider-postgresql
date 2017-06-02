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
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.TransactionCloseJob;
import com.gurucue.recommendations.TransactionalEntity;
import com.gurucue.recommendations.TransactionalEntityFactory;
import com.gurucue.recommendations.caching.product.EntityModifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A messy implementation of a transactional cache, it needs a reimplementation.
 * TODO: reimplement transactional semantics with copy-on-write behaviour, where copy owner is the thread doing the mutation.
 * In case of a TreeMap this should be done with a TreeMap implementation that supports one thread "next" pointer from a node,
 * plus a default pointer that represents the currently committed state.
 * In case of a HashMap this should be done with a HashMap implementation that supports one thread entity pointer per bucket
 * cell, plus a default pointer that represents the currently committed state.
 * Every mutation works on thread's entities, which become part of the committed structure on commit. Some locking must
 * be present: only one thread pointer per item can be present, so threads do not mess up each other. Deadlocks must be
 * detected and prevented with an exception.
 * Such an architecture correctly reflects all the changes to the working thread immediately, even in indices. The current
 * implementation doesn't.
 */
public final class TransactionalCache<K, V extends Id<?>> {
    private static final Logger log = LogManager.getLogger(TransactionalCache.class);

    private final ConcurrentMap<K, TransactionalEntity<V>> map;
    private final TransactionalEntityFactory<V> entityFactory;
    private volatile Index<?, V>[] indexArray;
    private volatile TransactionalIndex<V>[] transactionalIndices;

    public TransactionalCache(final TransactionalEntityFactory<V> entityFactory) {
        map = new ConcurrentHashMap<>(300000);
        this.entityFactory = entityFactory;
        indexArray = new Index[0];
        transactionalIndices = new TransactionalIndex[0];
    }

    public TransactionalCache(final TransactionalEntityFactory<V> entityFactory, final ConcurrentMap<K, TransactionalEntity<V>> initialContent, final Index<?, V>[] indices, final TransactionalIndex<V>[] transactionalIndices) {
        map = initialContent;
        this.entityFactory = entityFactory;
        final Index<?, V>[] idx = this.indexArray = indices == null ? new Index[0] : indices;
        final TransactionalIndex<V>[] trIdx = this.transactionalIndices = transactionalIndices == null ? new TransactionalIndex[0] : transactionalIndices;

        // initialize indices
        final Collection<TransactionalEntity<V>> entityValues = map.values();
        final List<V> values = new ArrayList<>(entityValues.size());
        for (final TransactionalEntity<? extends V> entity : entityValues) {
            if (entity == null) continue;
            final V value = entity.getCurrentValue();
            if (value != null) values.add(value);
        }

        for (int i = idx.length - 1; i >= 0; i--) {
            try {
                idx[i].replaceAll(values);
            }
            catch (Throwable t) {
                log.error("Initial index build failed: " + t.toString(), t);
            }
        }
        for (int i = trIdx.length - 1; i >= 0; i--) {
            try {
                trIdx[i].bulkFill(entityValues);
            }
            catch (Throwable t) {
                log.error("Initial transactional index build failed: " + t.toString(), t);
            }
        }
    }

    public TransactionalCache(final TransactionalEntityFactory<V> entityFactory, final int initialCapacity, final Index<?, V>[] indices, final TransactionalIndex<V>[] transactionalIndices) {
        this(entityFactory, new ConcurrentHashMap<K, TransactionalEntity<V>>(initialCapacity), indices, transactionalIndices);
    }

    public void putCommitted(final Transaction transaction, final K key, final V value) {
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, false);
        final V oldValue = entity.getCurrentValue();
        entity.setCurrentValue(value);
        changed(oldValue, entity, indexArray, transactionalIndices);
        entity.unlock(transaction);
    }

    /**
     * Atomically modifies an entity outside a transaction. The given
     * transaction is used only for locking accounting. It is the
     * responsibility of the modifier that the entity ID remains the
     * same.
     *
     * @param transaction the transaction to use
     * @param key the key of the value
     * @param modifier the modifier that will be invoked to perform the modification of the value with the given key
     */
    public void modifyCommitted(final Transaction transaction, final K key, final EntityModifier<V> modifier) {
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, true);
        if (entity == null) return;
        try {
            final V oldValue = entity.getCurrentValue();
            final V newValue = modifier.modify(oldValue);
            if (newValue == oldValue) return; // nothing to do
            entity.setCurrentValue(newValue);
            changed(oldValue, entity, indexArray, transactionalIndices);
            if (newValue == null) {
                map.remove(key, entity);
            }
        }
        finally {
            entity.unlock(transaction);
        }
    }

    /**
     * A fast putter, done without locking, not considering any previous values.
     * WARNING: this is not thread-safe!
     * This method is meant for use at initialization time when only a single
     * thread can perform cache access, to fill up an empty cache in the
     * shortest time possible.
     *
     * <p>
     *     THIS WILL FAIL VERY UGLY WHEN USED IN A MULTI-THREADED ENVIRONMENT OR WHEN ATTEMPTING MODIFICATION!
     * </p>
     *
     * @param key the key under which to put the value into the cache
     * @param value the value to be put into the cache
     */
    public void putFast(final K key, final V value) {
        final TransactionalEntity<V> entity = entityFactory.createCommitted(value);
        map.put(key, entity);
        changed(null, entity, indexArray, transactionalIndices);
    }

    public void putInTransaction(final Transaction transaction, final K key, final V value) {
        // we have a mapped entity; queue transaction commit/rollback job
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, false);
        entity.setNewValue(value);
        transaction.onTransactionClose(new PutJob<>(transaction, this, key, entity));
    }

    private TransactionalEntity<V> getLockedEntity(final K key, final Transaction transaction, final boolean nullable) {
        // ensure entity is mapped and locked
        final TransactionalEntity<V> entity = map.get(key);
        if (entity != null) {
            entity.lock(transaction); // obtain the lock
            if (entity.getCurrentValue() == null) { // entity is not mapped anymore, after we obtained the lock
                if (map.remove(key, entity)) {
                    log.error("A committed null entry was found @" + Integer.toHexString(entity.writeLock.hashCode()) + ", removed it");
                }
                entity.unlock(transaction);
                if (nullable) return null;
            }
            else {
                return entity;
            }
        }
        else if (nullable) return null;
        final TransactionalEntity<V> newEntity = entityFactory.createInTransaction(/*value*/null, transaction);
        for (; ; ) {
            final TransactionalEntity<V> oldEntity = map.putIfAbsent(key, newEntity);
            if (oldEntity != null) {
                oldEntity.lock(transaction);
                if (oldEntity.getCurrentValue() != null) {
                    // entity is mapped
                    newEntity.unlock(transaction);
                    return oldEntity;
                }
                else if (map.remove(key, oldEntity)) {
                    log.error("A committed null entry was found @" + Integer.toHexString(oldEntity.writeLock.hashCode()) + ", removed it");
                }
                // here we know the entity is not mapped anymore: revert lock and repeat the loop
                oldEntity.unlock(transaction);
            } else {
                // the new entity was mapped successfully
                return newEntity;
            }
        }
    }

    public V getCurrent(final K key) {
        final TransactionalEntity<V> entity = map.get(key);
        if (entity == null) return null;
        return entity.getCurrentValue();
    }

    public V getCurrentLocked(final Transaction transaction, final K key) {
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, false);
        transaction.onTransactionClose(new TransactionalEntityUnlockJob<>(transaction, entity));
        return entity.getCurrentValue();
    }

    public V removeCommitted(final Transaction transaction, final K key) {
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, true);
        if (entity == null) return null;
        final V value = entity.getCurrentValue();
        entity.setCurrentValue(null);
        map.remove(key, entity);
        changed(value, null, indexArray, transactionalIndices);
        entity.unlock(transaction);
        return value;
    }

    public V removeInTransaction(final Transaction transaction, final K key) {
        final TransactionalEntity<V> entity = getLockedEntity(key, transaction, true);
        if (entity == null) return null;
        entity.setNewValue(null);
        transaction.onTransactionClose(new PutJob<>(transaction, this, key, entity));
        return entity.getCurrentValue();
    }

    public List<V> removeInTransaction(final Transaction transaction, final Collection<K> keys) {
        final PutManyJob<K, V> job = new PutManyJob<>(transaction, this);
        final List<V> removedValues = new ArrayList<>(keys.size());
        for (final K key : keys) {
            final TransactionalEntity<V> entity = getLockedEntity(key, transaction, true);
            if (entity == null) continue;
            final V current = entity.getCurrentValue();
            if (current == null) continue;
            removedValues.add(current);
            entity.setNewValue(null);
            job.addTuple(key, entity);
        }
        transaction.onTransactionClose(job);
        return removedValues;
    }

    /**
     * Remove the given entity with the given key from the cache at transaction commit.
     * This method comes in handy when the entity is already present, such as
     * a result of an index search. The entity should already be locked to ensure
     * atomicity of the whole operation.
     *
     * @param transaction transaction to use
     * @param key key of the given entity
     * @param entity the entity that should be removed from the cache
     */
    public void removeExistingInTransaction(final Transaction transaction, final K key, final TransactionalEntity<V> entity) {
        entity.lock(transaction); // increase the lock count, because PutJob decreases it
        entity.setNewValue(null);
        transaction.onTransactionClose(new PutJob<>(transaction, this, key, entity));
    }

    private void changed(final V before, final TransactionalEntity<V> afterEntity, final Index<?, V>[] indices, final TransactionalIndex<V>[] transactionalIndices) {
        synchronized (this) {
            final V after = afterEntity == null ? null : afterEntity.getCurrentValue();
            for (int i = indices.length - 1; i >= 0; i--) {
                try {
                    indices[i].changed(before, after);
                } catch (Throwable t) {
                    log.error("Index modification failed: " + t.toString(), t);
                }
            }
            for (int i = transactionalIndices.length - 1; i >= 0; i--) {
                try {
                    transactionalIndices[i].changed(before, afterEntity);
                } catch (Throwable t) {
                    log.error("Transactional index modification failed: " + t.toString(), t);
                }
            }
        }
    }

    public void addIndex(final Index<?, V> index) {
        if (index == null) return;
        synchronized (this) {
            final Collection<TransactionalEntity<V>> entityValues = map.values();
            final List<V> values = new ArrayList<>(entityValues.size());
            for (final TransactionalEntity<V> entity : entityValues) {
                if (entity == null) continue;
                final V value = entity.getCurrentValue();
                if (value != null) values.add(value);
            }
            final Index<?, V>[] oldIndices = indexArray;
            final int n = oldIndices.length;
            final Index<?, V>[] newIndices = Arrays.copyOf(oldIndices, n+1);
            newIndices[n] = index;
            index.replaceAll(values);
            indexArray = newIndices;
        }
    }

    public void addIndex(final TransactionalIndex<V> index) {
        if (index == null) return;
        synchronized (this) {
            final TransactionalIndex<V>[] oldIndices = transactionalIndices;
            final int n = oldIndices.length;
            final TransactionalIndex<V>[] newIndices = Arrays.copyOf(oldIndices, n+1);
            newIndices[n] = index;
            index.bulkFill(map.values());
            transactionalIndices = newIndices;
        }
    }

    public void destroy() {
        final Index<?, V>[] indices;
        synchronized (this) {
            indices = indexArray;
            indexArray = new Index[0];
            map.clear();
        }
        for (int i = indices.length - 1; i >= 0; i--) {
            indices[i].destroy();
        }
    }

    public int size() {
        return map.size();
    }

    private static final class PutJob<K, V extends Id<?>> implements TransactionCloseJob {
        private final TransactionalCache<K, V> cache;
        private final K key;
        private final TransactionalEntity<V> entity;
        private final Transaction transaction;

        PutJob(final Transaction transaction, final TransactionalCache<K, V> cache, final K key, final TransactionalEntity<V> entity) {
            this.transaction = transaction;
            this.cache = cache;
            this.key = key;
            this.entity = entity;
        }

        @Override
        public void commit() {
            try {
                final V oldValue = entity.getCurrentValue();
                entity.setCurrentValue(entity.getNewValue());
                entity.setNewValue(null);
                cache.changed(oldValue, entity, cache.indexArray, cache.transactionalIndices);
                if (entity.getCurrentValue() == null) {
                    cache.map.remove(key, entity);
                }
            }
            finally {
                entity.unlock(transaction);
            }
        }

        @Override
        public void rollback() {
            try {
                entity.setNewValue(null);
                if (entity.getCurrentValue() == null) {
                    cache.map.remove(key, entity);
                }
            }
            finally {
                entity.unlock(transaction);
            }
        }
    }

    private static final class PutManyJob<K, V extends Id<?>> implements TransactionCloseJob {
        private static final Logger log = LogManager.getLogger(PutManyJob.class);

        private final Transaction transaction;
        private final TransactionalCache<K, V> cache;
        private final List<Tuple<K, V>> tuples = new ArrayList<>();

        PutManyJob(final Transaction transaction, final TransactionalCache<K, V> cache) {
            this.transaction = transaction;
            this.cache = cache;
        }

        @Override
        public void commit() {
            // optimized for deletions
            for (final Tuple<K, V> tuple: tuples) {
                final TransactionalEntity<V> entity = tuple.entity;
                try {
                    final V newValue = entity.getNewValue();
                    if (newValue == null) {
                        entity.setCurrentValue(null);
                        cache.changed(null, entity, cache.indexArray, cache.transactionalIndices);
                        cache.map.remove(tuple.key, entity);
                    }
                    else {
                        final V oldValue = entity.getCurrentValue();
                        entity.setCurrentValue(newValue);
                        entity.setNewValue(null);
                        cache.changed(oldValue, entity, cache.indexArray, cache.transactionalIndices);
                    }
                }
                catch (RuntimeException e) {
                    log.error("Exception during commit of a batch of product changes: " + e.toString(), e);
                }
                finally {
                    entity.unlock(transaction);
                }
            }
        }

        @Override
        public void rollback() {
            for (final Tuple<K, V> tuple: tuples) {
                final TransactionalEntity<V> entity = tuple.entity;
                try {
                    entity.setNewValue(null);
                    if (entity.getCurrentValue() == null) {
                        cache.map.remove(tuple.key, entity);
                    }
                }
                catch (RuntimeException e) {
                    log.error("Exception during rollback of a batch of product changes: " + e.toString(), e);
                }
                finally {
                    entity.unlock(transaction);
                }
            }
        }

        void addTuple(final K key, final TransactionalEntity<V> entity) {
            tuples.add(new Tuple<>(key, entity));
        }

        private static final class Tuple<K, V extends Id<?>> {
            private final K key;
            private final TransactionalEntity<V> entity;
            Tuple(final K key, final TransactionalEntity<V> entity) {
                this.key = key;
                this.entity = entity;
            }
        }
    }
}
