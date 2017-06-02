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

import com.gurucue.recommendations.data.DataLink;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.dto.ConsumerEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public final class ConsumerCache {
    private static final Logger log = LogManager.getLogger(ConsumerCache.class);

    final SimpleCache<Long, ConsumerCacheEntity> cache;

    final UniqueIndex<KeyPartnerIdAndUsernameAndTypeAndParent, ConsumerCacheEntity> indexByPartnerAndUsernameAndTypeAndParent;
    final CommonIndex<Long, Long, ConsumerCacheEntity> indexByParentId;

    public ConsumerCache(final DataProvider physicalProvider) {
        final long startNano = System.nanoTime();
        log.info("Creating consumer cache");
        cache = new SimpleCache<>();

        indexByPartnerAndUsernameAndTypeAndParent = UniqueIndex.newHashIndex(new PartnerAndUsernameAndTypeAndParentIndexDefinition());
        indexByParentId = CommonIndex.newHashIndex(new ParentIdIndexDefinition());
        cache.addIndex(indexByPartnerAndUsernameAndTypeAndParent);
        cache.addIndex(indexByParentId);

        // fill cache (read data into memory)
        DataLink link = physicalProvider.newDataLink();
        try {
            fillCache(link);
        }
        finally {
            link.close();
        }

        final long endNano = System.nanoTime();
        log.info("Consumer cache created and initialized in " + (endNano - startNano) + " ns");
    }

    private void fillCache(final DataLink link) {
        log.debug("Caching consumers");

        int consumersWithRelations = 0;
        final List<ConsumerEntity> consumers = link.getConsumerManager().list();
        for (final ConsumerEntity c : consumers) {
            if (c.relations.size() > 0) consumersWithRelations++;
            ConsumerCacheEntity.create(c).save(this);
        }

        log.debug(consumers.size() + " consumers cached, " + consumersWithRelations + " consumers having relation(s)");
    }

    // API methods

    public void destroy() {
        cache.destroy(); // the cache also destroys all its indexes
        log.info("Consumer cache destroyed");
    }

    public ConsumerEntity getConsumer(final Long id) {
        final ConsumerCacheEntity cacheEntity = cache.get(id);
        if (cacheEntity == null) return null;
        return cacheEntity.consumer;
    }

    public ConsumerEntity getConsumer(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId) {
        final ConsumerCacheEntity cacheEntity = indexByPartnerAndUsernameAndTypeAndParent.find(new KeyPartnerIdAndUsernameAndTypeAndParent(partnerId, username, consumerTypeId, parentConsumerId));
        if (cacheEntity == null) return null;
        return cacheEntity.consumer;
    }

    public ConsumerEntity removeConsumer(final Long id) {
        final ConsumerCacheEntity cacheEntity = cache.get(id);
        if (cacheEntity == null) return null;
        final ConsumerCacheEntity removedEntity = cacheEntity.delete(this);
        if (removedEntity == null) return null;
        // also remove any children
        removeChildren(removedEntity.getId());
        return removedEntity.consumer;
    }

    public void storeConsumer(final ConsumerEntity consumer) {
        ConsumerCacheEntity.create(consumer).save(this);
    }

    /**
     * Used when resetting consumer history: a consumer with new ID is created.
     * @param newConsumer the consumer to which to add/merge new children
     * @param newChildren the children to add/merge to the consumer
     */
    public void replaceConsumer(final ConsumerEntity newConsumer, final List<ConsumerEntity> newChildren) {
        final ConsumerCacheEntity oldEntity = indexByPartnerAndUsernameAndTypeAndParent.find(new KeyPartnerIdAndUsernameAndTypeAndParent(newConsumer.partnerId, newConsumer.username, newConsumer.consumerTypeId, newConsumer.parentId));
        if (oldEntity != null) {
            if (oldEntity.getId().longValue() != newConsumer.id) {
                addChildren(newChildren);
                ConsumerCacheEntity.create(newConsumer).save(this);
                oldEntity.delete(this);
                removeChildren(oldEntity.getId());
            }
            else {
                replaceChildren(newConsumer.id, newChildren);
                ConsumerCacheEntity.create(newConsumer).save(this);
            }
        }
        else {
            addChildren(newChildren);
            ConsumerCacheEntity.create(newConsumer).save(this);
        }
    }

    void removeChildren(final Long parentId) {
        final Collection<ConsumerCacheEntity> firstChildren = indexByParentId.find(parentId);
        if ((firstChildren == null) || (firstChildren.isEmpty())) return;
        final LinkedList<ConsumerCacheEntity> children = new LinkedList<>(firstChildren);
        while (!children.isEmpty()) {
            final ConsumerCacheEntity child = children.removeFirst();
            if (child != null) {
                child.delete(this);
                if (child.consumer != null) children.addAll(indexByParentId.find(child.getId()));
            }
        }
    }

    /**
     * Replaces the children belonging to the given parent ID with the
     * specified list of new children.
     *
     * TODO: handles only first cascade, add cascading replacement
     *
     * @param parentId the ID of the parent consumer
     * @param newChildren the collection of replacement children
     */
    void replaceChildren(final Long parentId, final List<ConsumerEntity> newChildren) {
        final Collection<ConsumerCacheEntity> c = indexByParentId.find(parentId);
        if ((c == null) || c.isEmpty()) {
            addChildren(newChildren);
            return;
        }
        if ((newChildren == null) || newChildren.isEmpty()) {
            removeChildren(parentId);
            return;
        }
        final Map<Long, ConsumerCacheEntity> oldChildren = new HashMap<>(c.size());
        for (final ConsumerCacheEntity e : c) oldChildren.put(e.getId(), e);
        for (final ConsumerEntity newChild : newChildren) {
            oldChildren.remove(newChild.id);
            ConsumerCacheEntity.create(newChild).save(this);
        }
        for (final ConsumerCacheEntity leftover : oldChildren.values()) {
            leftover.delete(this);
        }
    }

    void addChildren(final List<ConsumerEntity> newChildren) {
        if ((newChildren == null) || newChildren.isEmpty()) return;
        for (final ConsumerEntity child : newChildren) {
            ConsumerCacheEntity.create(child).save(this);
        }
    }

    public List<ConsumerEntity> list() {
        final Iterator<ConsumerCacheEntity> values = cache.values().iterator();
        if (!values.hasNext()) return Collections.emptyList();
        final List<ConsumerEntity> result = new ArrayList<>();
        result.add(values.next().consumer);
        while (values.hasNext()) {
            result.add(values.next().consumer);
        }
        return result;
    }

    public List<ConsumerEntity> getActiveChildren(final Long parentId) {
        final Collection<ConsumerCacheEntity> children = indexByParentId.find(parentId);
        if ((children == null) || (children.isEmpty())) return Collections.emptyList();
        final List<ConsumerEntity> result = new ArrayList<>(children.size());
        for (final ConsumerCacheEntity entity : children) {
            final ConsumerEntity child = entity.consumer;
            if (child == null) continue;
            if (child.deleted > 0L) continue;
            result.add(child);
        }
        return result;
    }

    public Collection<ConsumerCacheEntity> getChildEntities(final Long parentId) {
        return indexByParentId.find(parentId);
    }

    // ==============================
    //        Index definitions
    // ==============================

    static class KeyPartnerIdAndUsernameAndTypeAndParent {
        final long partnerId;
        final String username;
        final long consumerTypeId;
        final long parentConsumerId;
        private final int hash;
        public KeyPartnerIdAndUsernameAndTypeAndParent(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId) {
            this.partnerId = partnerId;
            this.username = username;
            this.consumerTypeId = consumerTypeId;
            this.parentConsumerId = parentConsumerId;
            // recipe taken from Effective Java, 2nd edition (ISBN 978-0-321-35668-0), page 47
            int hash = 17;
            hash = (31 * hash) + (int)(partnerId ^ (partnerId >>> 32));
            hash = (31 * hash) + username.hashCode();
            hash = (31 * hash) + (int)(consumerTypeId ^ (consumerTypeId >>> 32));
            hash = (31 * hash) + (int)(parentConsumerId ^ (parentConsumerId >>> 32));
            this.hash = hash;
        }
        @Override
        public boolean equals(final Object obj) {
            if (obj == null) return false;
            if (obj instanceof KeyPartnerIdAndUsernameAndTypeAndParent) {
                final KeyPartnerIdAndUsernameAndTypeAndParent other = (KeyPartnerIdAndUsernameAndTypeAndParent) obj;
                if (other.partnerId != this.partnerId) return false;
                if (other.consumerTypeId != this.consumerTypeId) return false;
                if (other.parentConsumerId != this.parentConsumerId) return false;
                if (username.equals(other.username)) return true;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return hash;
        }
    }

    static class PartnerAndUsernameAndTypeAndParentIndexDefinition extends UnikeyIndexDefinition<KeyPartnerIdAndUsernameAndTypeAndParent, ConsumerCacheEntity> {
        @Override
        public KeyPartnerIdAndUsernameAndTypeAndParent key(final ConsumerCacheEntity entity) {
            if (entity == null) return null;
            final ConsumerEntity consumer = entity.consumer;
            if ((consumer == null) || (consumer.partnerId < 0L) || (consumer.username == null)) return null;
            return new KeyPartnerIdAndUsernameAndTypeAndParent(consumer.partnerId, consumer.username, consumer.consumerTypeId, consumer.parentId);
        }
    }

    static class ParentIdIndexDefinition extends UnikeyIndexDefinition<Long, ConsumerCacheEntity> {
        @Override
        public Long key(final ConsumerCacheEntity entity) {
            if (entity == null) return null;
            final long parentId = entity.consumer.parentId;
            if (parentId == 0L) return null;
            return parentId;
        }
    }
}
