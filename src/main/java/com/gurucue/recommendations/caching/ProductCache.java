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

import com.google.common.collect.ImmutableMap;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.caching.product.AttributeIds;
import com.gurucue.recommendations.caching.product.EntityModifier;
import com.gurucue.recommendations.caching.product.KeyAttributes;
import com.gurucue.recommendations.caching.product.RelatedIndexer;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import com.gurucue.recommendations.entity.value.Value;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public abstract class ProductCache<V extends Product> {
    public static final TvProgrammeProduct TV_PROGRAMME_TIME_OUT_OF_RANGE = new TvProgrammeProduct();
    /**
     * Retrieve a Product from cache by our internal ID.
     *
     * @param id a product ID
     * @return the product instance with the specified product ID
     */
    public abstract V getProduct(Long id);

    public abstract V getProductLocked(Transaction transaction, Long id);

    public abstract V removeProduct(Transaction transaction, Long id);

    public abstract void putProductCommitted(Transaction transaction, V product);

    /**
     * Return the data provider used by this cache.
     *
     * @return the data provider used by this cache
     */
    public abstract DataProvider getProvider();

    protected abstract TransactionalCache<Long, V> getCache();

    protected abstract Logger getLog();

    public void setRelatedFieldForAll(final Transaction transaction, final Attribute attribute, final Value oldValue, final Value newValue) {
        final List<V> existingProducts = findByVariantRelatedIndex(Collections.singletonMap(attribute, oldValue));
        if ((existingProducts == null) || (existingProducts.isEmpty())) return;
        // TODO: in the time between obtaining index results and locking them it is possible for an item to be modified and fall out of index
        final TransactionalCache<Long, V> cache = getCache();
        final DataProvider provider = getProvider();
        final ImmutableMap<Attribute, Value> newAttributeValueMap = ImmutableMap.of(attribute, newValue);
        final EntityModifier modifier = new EntityModifier<V>() {
            @Override
            public V modify(final V entity) {
                if (!oldValue.equals(entity.related.get(attribute))) return entity;
                final Product newProduct = Product.create(entity.id, entity.productTypeId, entity.partnerId, entity.partnerProductCode, entity.added, entity.modified, entity.deleted, entity.attributes, entity.related.modify(newAttributeValueMap, null), provider);
                try {
                    return (V) newProduct;
                }
                catch (ClassCastException e) {
                    getLog().error("setRelatedFieldForAll(): the new Product instance is not of the same type as the old instance: " + newProduct.getClass().getCanonicalName() + " != " + oldValue.getClass().getCanonicalName() + ": " + e.toString(), e);
                    return entity;
                }
            }
        };
        for (final V p : existingProducts) {
            cache.modifyCommitted(transaction, p.id, modifier);
        }
    }

    private final ConcurrentMap<AttributeIds, Future<CommonIndex<KeyAttributes, Long, V>>> variantRelatedIndices = new ConcurrentHashMap<>();

    protected CommonIndex<KeyAttributes, Long, V> getVariantRelatedIndex(final AttributeIds attributeIds) {
        for (;;) {
            Future<CommonIndex<KeyAttributes, Long, V>> f = variantRelatedIndices.get(attributeIds);
            if (f == null) {
                FutureTask<CommonIndex<KeyAttributes, Long, V>> ft = new FutureTask<>(new VariantRelatedIndexCreator<>(getCache(), attributeIds, getProvider().getAttributeCodes()));
                f = variantRelatedIndices.putIfAbsent(attributeIds, ft);
                if (f == null) {
                    getLog().info("Creating new \"related\" variant index: " + attributeIds.toString());
                    f = ft;
                    ft.run();
                }
            }
            try {
                return f.get();
            }
            catch (CancellationException e) {
                variantRelatedIndices.remove(attributeIds, f);
            }
            catch (ExecutionException e) {
                variantRelatedIndices.remove(attributeIds, f);
                throw new IllegalStateException("Variant \"related\" index computation failed: " + e.toString(), e);
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted while waiting for \"related\" variant index: " + e.toString(), e);
            }
        }
    }

    public List<V> findByVariantRelatedIndex(final Map<Attribute, Value> relatedValues) {
        final Logger log = getLog();
        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder(150 + (relatedValues.size() * 40)); // guesstimate
            sb.append("findByVariantRelatedIndex(): using \"related\" variant index with values: ");
            final Iterator<Map.Entry<Attribute, Value>> entryIterator = relatedValues.entrySet().iterator();
            if (entryIterator.hasNext()) {
                Map.Entry<Attribute, Value> entry = entryIterator.next();
                sb.append(entry.getKey().getIdentifier());
                sb.append("=");
                entry.getValue().toJson(sb);
                while (entryIterator.hasNext()) {
                    entry = entryIterator.next();
                    sb.append(", ");
                    sb.append(entry.getKey().getIdentifier());
                    sb.append("=");
                    entry.getValue().toJson(sb);
                }
            }
            log.debug(sb.toString());
        }

        final AttributeIds ids = new AttributeIds(relatedValues.keySet());
        final List<KeyAttributes> keys = KeyAttributes.createKeys(ids, relatedValues, getProvider().getAttributeCodes());
        final List<V> result;
        final int keySize = keys.size();
        if (keySize == 0) {
            result = Collections.emptyList();
        }
        else if (keySize == 1) {
            // optimization step: majority of cases will result in only one key
            result = new ArrayList<>(getVariantRelatedIndex(ids).find(keys.get(0)));
        }
        else {
            // collect products for all keys through mapping by product IDs, to remove any duplicates
            final Map<Long, V> resultMap = new HashMap<>();
            final CommonIndex<KeyAttributes, Long, V> index = getVariantRelatedIndex(ids);
            for (final KeyAttributes key : keys) {
                for (final V p : index.find(key)) {
                    resultMap.put(p.id, p);
                }
            }
            result = new ArrayList<>(resultMap.values());
        }

        if (log.isDebugEnabled()) {
            if (result.size() == 0) log.debug("findByVariantRelatedIndex(): no results");
            else {
                final StringBuilder sb = new StringBuilder(60 + (result.size() * 11)); // guesstimate
                sb.append("findByVariantRelatedIndex(): found ");
                sb.append(result.size());
                sb.append(" results: ");
                final Iterator<V> it = result.iterator();
                V p = it.next();
                sb.append(p.id);
                while (it.hasNext()) {
                    sb.append(", ");
                    sb.append(it.next().id);
                }
                log.debug(sb.toString());
            }
        }
        return result;
    }

    static final class VariantRelatedIndexCreator<V extends Product> implements Callable<CommonIndex<KeyAttributes, Long, V>> {
        private final AttributeIds relatedIds;
        private final TransactionalCache<Long, V> cache;
        private final AttributeCodes attributeCodes;

        VariantRelatedIndexCreator(final TransactionalCache<Long, V> cache, final AttributeIds relatedIds, final AttributeCodes attributeCodes) {
            this.cache = cache;
            this.relatedIds = relatedIds;
            this.attributeCodes = attributeCodes;
        }

        @Override
        public CommonIndex<KeyAttributes, Long, V> call() throws Exception {
            final CommonIndex<KeyAttributes, Long, V> index = CommonIndex.newHashIndex(new RelatedIndexer<V>(relatedIds, attributeCodes));
            cache.addIndex(index);
            return index;
        }
    }
}
