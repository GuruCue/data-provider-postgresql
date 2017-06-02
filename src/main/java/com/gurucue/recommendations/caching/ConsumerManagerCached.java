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

import com.gurucue.recommendations.dto.ConsumerEntity;
import com.gurucue.recommendations.dto.RelationConsumerProductEntity;
import com.gurucue.recommendations.entitymanager.ConsumerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public final class ConsumerManagerCached implements ConsumerManager {
    private static final Logger log = LogManager.getLogger(ProductManagerCached.class);

    private final CachedJdbcDataLink dataLink;
    private final ConsumerCache consumerCache;
    private ConsumerManager physicalManager = null;

    public ConsumerManagerCached(final CachedJdbcDataLink dataLink, final ConsumerCache consumerCache) {
        this.dataLink = dataLink;
        this.consumerCache = consumerCache;
    }

    private ConsumerManager getPhysicalManager() {
        // lazy init: we don't need physical provider for cached data
        if (physicalManager == null) physicalManager = dataLink.getPhysicalLink().getConsumerManager();
        return physicalManager;
    }

    @Override
    public ConsumerEntity getById(final long id) {
        final ConsumerEntity cachedEntity = consumerCache.getConsumer(id);
        if (cachedEntity != null) return cachedEntity;
        final ConsumerEntity dbEntity = getPhysicalManager().getById(id);
        if (dbEntity == null) return null;
        log.warn("Un-cached consumer found, caching it: " + dbEntity);
        consumerCache.addChildren(getActiveChildren(dbEntity.id, false));
        consumerCache.storeConsumer(dbEntity);
        return dbEntity;
    }

    @Override
    public ConsumerEntity getByPartnerIdAndUsernameAndTypeAndParent(final long partnerId, final String username, final long consumerTypeId, final long parentId) {
        final ConsumerEntity cachedEntity = consumerCache.getConsumer(partnerId, username, consumerTypeId, parentId);
        if (cachedEntity != null) return cachedEntity;
        final ConsumerEntity dbEntity = getPhysicalManager().getByPartnerIdAndUsernameAndTypeAndParent(partnerId, username, consumerTypeId, parentId);
        if (dbEntity == null) return null;
        log.warn("Un-cached consumer found, caching it: " + dbEntity);
        consumerCache.addChildren(getActiveChildren(dbEntity.id, false));
        consumerCache.storeConsumer(dbEntity);
        return dbEntity;
    }

    @Override
    public List<ConsumerEntity> list() {
        return consumerCache.list();
    }

    @Override
    public ConsumerEntity merge(final long partnerId, final String username, final boolean resetEvents, final List<RelationConsumerProductEntity> relations, final long consumerTypeId, final long parentId) {
        final ConsumerManager cm = getPhysicalManager();
        final ConsumerEntity resultingEntity = cm.merge(partnerId, username, resetEvents, relations, consumerTypeId, parentId);
        final List<ConsumerEntity> children;
        if (resultingEntity == null) children = Collections.emptyList();
        else children = cm.getActiveChildren(resultingEntity.id, false);
        dataLink.addCommitJob(new SaveConsumer(consumerCache, partnerId, username, resultingEntity, children));
        return resultingEntity;
    }

    @Override
    public ConsumerEntity update(final long partnerId, final String username, final boolean resetEvents, final List<RelationConsumerProductEntity> relations, final long consumerTypeId, final long parentId) {
        final ConsumerManager cm = getPhysicalManager();
        final ConsumerEntity resultingEntity = cm.update(partnerId, username, resetEvents, relations, consumerTypeId, parentId);
        final List<ConsumerEntity> children;
        if (resultingEntity == null) children = Collections.emptyList();
        else children = cm.getActiveChildren(resultingEntity.id, false);
        dataLink.addCommitJob(new SaveConsumer(consumerCache, partnerId, username, resultingEntity, children));
        return resultingEntity;
    }

    // TODO: after some time purge consumers that were deleted with delayed anonymization
    @Override
    public ConsumerEntity delete(final long partnerId, final String username, final long consumerTypeId, final long parentId, final boolean delayedAnonymization) {
        final ConsumerManager cm = getPhysicalManager();
        final ConsumerEntity deletedEntity = cm.delete(partnerId, username, consumerTypeId, parentId, delayedAnonymization);
        if (deletedEntity != null) {
            if (deletedEntity.status == 0) dataLink.addCommitJob(new DeleteConsumer(consumerCache, deletedEntity.id));
            else {
                final List<ConsumerEntity> children;
                if (deletedEntity == null) children = Collections.emptyList();
                else children = cm.getActiveChildren(deletedEntity.id, false);
                dataLink.addCommitJob(new SaveConsumer(consumerCache, partnerId, username, deletedEntity, children));
            }
        }
        return deletedEntity;
    }

    @Override
    public List<ConsumerEntity> getActiveChildren(final long consumerId, final boolean forUpdate) {
        return consumerCache.getActiveChildren(consumerId);
    }

    static final class SaveConsumer implements Runnable {
        final ConsumerCache cache;
        final long partnerId;
        final String username;
        final ConsumerEntity newConsumer;
        final List<ConsumerEntity> newChildren;

        SaveConsumer(final ConsumerCache cache, final long partnerId, final String username, final ConsumerEntity newConsumer, final List<ConsumerEntity> newChildren) {
            this.cache = cache;
            this.partnerId = partnerId;
            this.username = username;
            this.newConsumer = newConsumer;
            this.newChildren = newChildren;
        }

        @Override
        public void run() {
            cache.replaceConsumer(newConsumer, newChildren);
        }
    }

    static final class DeleteConsumer implements Runnable {
        final ConsumerCache cache;
        final long consumerId;

        DeleteConsumer(final ConsumerCache cache, final long consumerId) {
            this.cache = cache;
            this.consumerId = consumerId;
        }

        @Override
        public void run() {
            cache.removeConsumer(consumerId);
        }
    }
}
