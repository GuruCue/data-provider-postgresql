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

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

public abstract class TransactionalUniqueIndex<K, V extends Id<?>> implements TransactionalIndex<V> {
    protected final ConcurrentMap<K, TransactionalEntity<V>> index;

    protected TransactionalUniqueIndex(final ConcurrentMap<K, TransactionalEntity<V>> index) {
        this.index = index;
    }

    public abstract void bulkFill(final Collection<TransactionalEntity<V>> entities);

    public abstract void changed(final V previousValue, final TransactionalEntity<V> currentEntity);

    /**
     * Returns the entity with the given key, or <code>null</code> if it does not exist.
     * No locking is performed.
     *
     * @param key the key of the entity
     * @return the entity with the given key
     */
    public TransactionalEntity<V> find(final K key) {
        return index.get(key);
    }

    /**
     * Returns the entity with the given key. The returned
     * entity is locked, so no other thread can access or modify it through lock-ful
     * methods. If <code>allowMissing == true</code>,
     * then if no such entity exists a <code>null</code> is returned, otherwise a
     * new entity under the given key and with a <code>null</code> value is created,
     * locked, and returned, so no other thread can create or modify it through
     * lock-ful methods until the transaction is committed or rolled back.
     *
     * @param transaction the transaction to use
     * @param key the key of the entity
     * @param allowMissing whether to return <code>null</code> if there is no entity with the given key
     * @return an entity with the given key; if no entity existed with the given key, then a new instance is created and returned, unless <code>allowMissing</code> was <code>true</code>
     */
    public TransactionalEntity<V> findAndLock(final Transaction transaction, final K key, final boolean allowMissing) {
        // optimistic path
        final TransactionalEntity<V> entity = index.get(key);
        if (entity != null) {
            entity.lock(transaction);
            if (entity.getCurrentValue() == null) {
                // it got removed, ditch it
                entity.unlock(transaction);
                if (allowMissing) return null;
            }
            else {
                transaction.onTransactionClose(new TransactionalEntityUnlockJob<>(transaction, entity));
                return entity;
            }
        }
        else if (allowMissing) return null;
        // pessimistic path
        final TransactionalEntity<V> newEntity = TransactionalEntity.createInTransaction(null, transaction);
        for (; ; ) {
            final TransactionalEntity<V> oldEntity = index.putIfAbsent(key, newEntity);
            if (oldEntity != null) {
                oldEntity.lock(transaction);
                if (oldEntity.getCurrentValue() != null) {
                    // entity is mapped
                    newEntity.unlock(transaction);
                    transaction.onTransactionClose(new TransactionalEntityUnlockJob<>(transaction, oldEntity));
                    return oldEntity;
                }
                // here we know the entity is not mapped anymore: revert lock and repeat the loop
                oldEntity.unlock(transaction);
            } else {
                // the new entity was mapped successfully
                transaction.onTransactionClose(new RemoveJob<>(transaction, this, key, newEntity));
                return newEntity;
            }
        }
    }

    static final class RemoveJob<K, V extends Id<?>> implements TransactionCloseJob {
        private final TransactionalUniqueIndex<K, V> index;
        private final K key;
        private final TransactionalEntity<V> entity;
        private final Transaction transaction;

        RemoveJob(final Transaction transaction, final TransactionalUniqueIndex<K, V> index, final K key, final TransactionalEntity<V> entity) {
            this.transaction = transaction;
            this.index = index;
            this.key = key;
            this.entity = entity;
        }

        @Override
        public void commit() {
            try {
                if (entity.getCurrentValue() == null) {
                    index.index.remove(key, entity);
                }
            }
            finally {
                entity.unlock(transaction);
            }
        }

        @Override
        public void rollback() {
            try {
                entity.setCurrentValue(null);
                index.index.remove(key, entity);
            }
            finally {
                entity.unlock(transaction);
            }
        }
    }
}
