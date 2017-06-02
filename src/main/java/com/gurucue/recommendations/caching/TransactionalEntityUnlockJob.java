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

public class TransactionalEntityUnlockJob<V extends Id<?>> implements TransactionCloseJob {
    private final TransactionalEntity<V> entity;
    private final Transaction transaction;

    public TransactionalEntityUnlockJob(final Transaction transaction, final TransactionalEntity<V> entity) {
        this.transaction = transaction;
        this.entity = entity;
    }

    @Override
    public void commit() {
        entity.unlock(transaction);
    }

    @Override
    public void rollback() {
        entity.unlock(transaction);
    }
}
