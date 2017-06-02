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

import com.google.common.collect.ImmutableSet;
import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.ResponseException;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.TransactionalEntity;
import com.gurucue.recommendations.TransactionalEntityFactory;
import com.gurucue.recommendations.caching.product.MatcherKeyKeyFactory;
import com.gurucue.recommendations.caching.product.ProductCreator;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.product.Matcher;
import com.gurucue.recommendations.entity.product.MatcherKey;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.SeriesMatch;
import com.gurucue.recommendations.entity.product.VideoMatch;
import com.gurucue.recommendations.entity.value.AttributeValues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TimeZone;

/**
 * Caches only products internal to the service, i.e. not visible to any partner.
 * Currently those are video-match and series-match products.
 */
public final class InternalProductCache extends ProductCache<Matcher> {
    private static final Logger log = LogManager.getLogger(InternalProductCache.class);

    // indices
    final TransactionalMultikeyCommonIndex<MatcherKey, Matcher> indexMatcherKey;

    // cache infrastructure
    final TransactionalCache<Long, Matcher> cache;
    final JdbcDataProvider dataProvider; // used in sanity checking, to retrieve any missing products
    final AttributeCodes attributeCodes;
    final ProductTypeCodes productTypeCodes;
    final long partnerId;

    public InternalProductCache(final long partnerId, final JdbcDataProvider provider) {
        this.partnerId = partnerId; // this should always be fixed at zero, but we parameterise it anyway
        this.dataProvider = provider;
        this.attributeCodes = provider.getAttributeCodes();
        this.productTypeCodes = provider.getProductTypeCodes();

        final String threadId = "[" + Thread.currentThread().getId() + "] ";
        final long startNano = System.nanoTime();
        log.info(threadId + "Creating internal products cache");
        try {
            // define indexes
            indexMatcherKey = TransactionalMultikeyCommonIndex.newHashIndex(new MatcherKeyKeyFactory());

            // define cache
            cache = new TransactionalCache<>(
                    new MatcherTransactionalEntityFactory(),
                    1000000,
                    new Index[0],
                    new TransactionalIndex[]{indexMatcherKey}
            );

            // initialize cache (fill it with content)
            try (final JdbcDataLink link = provider.newJdbcDataLink()) {
                fillCache(link);
            }
        }
        catch (SQLException|ResponseException e) {
            throw new DatabaseException("Failed to create internal products cache because of a database error: " + e.toString(), e);
        }

        final long endNano = System.nanoTime();
        log.info(threadId + "Internal products cache created and initialized in " + (endNano - startNano) + " ns with " + cache.size() + " elements");
    }

    private void fillCache(final JdbcDataLink link) throws SQLException, ResponseException {
        final String threadId = "[" + Thread.currentThread().getId() + "] ";
        log.info(threadId + "Filling internal products cache");
        final StringBuilder logBuilder = new StringBuilder(16384);

        final ProductCreator[] productCreators = {
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.video;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        return new VideoMatch(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.series;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        return new SeriesMatch(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                }
        };

        final StringBuilder fillErrors = new StringBuilder(10000);
        fillErrors.append(threadId).append("Internal cache initialization errors, ignoring:");
        final int startingLogSize = fillErrors.length();
        final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        try (final PreparedStatement st = link.prepareStatement("select p.id, p.partner_product_code, p.added, p.modified, p.deleted, p.attributes::text, p.related::text from product p where p.partner_id = ? and p.product_type_id = ? and p.deleted is null", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(2000);
            for (int i = 0; i < productCreators.length; i++) {
                final ProductCreator creator = productCreators[i];
                final ProductType productType = creator.getProductType();
                log.info(threadId + "Reading and caching products of type " + productType.getIdentifier());
                st.setLong(1, partnerId);
                st.setLong(2, productType.getId());
                int readCount = 0;
                int cachedCount = 0;
                try (final ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        readCount++;
                        final long id = rs.getLong(1);
                        String partnerProductCode = rs.getString(2);
                        if (rs.wasNull()) partnerProductCode = null;
                        Timestamp added = rs.getTimestamp(3, utcCalendar);
                        if (rs.wasNull()) added = null;
                        Timestamp modified = rs.getTimestamp(4, utcCalendar);
                        if (rs.wasNull()) modified = null;
                        Timestamp deleted = rs.getTimestamp(5, utcCalendar);
                        if (rs.wasNull()) deleted = null;
                        String jsonAttributes = rs.getString(6);
                        if (rs.wasNull()) jsonAttributes = null;
                        String jsonRelated = rs.getString(7);
                        if (rs.wasNull()) jsonRelated = null;
                        final Product p = creator.create(id, partnerProductCode, jsonAttributes, jsonRelated, added, modified, deleted);
                        try {
                            cache.putFast(id, (Matcher) p);
                            cachedCount++;
                        }
                        catch (ClassCastException e) {
                            fillErrors.append("\n  ").append("Invalid product type: not a matcher type: ").append(p.getClass().getCanonicalName()).append(": ");
                            p.toString(fillErrors);
                        }
                    }
                }
                if (startingLogSize < fillErrors.length()) log.error(fillErrors.toString());
                log.info(threadId + "Read " + readCount + " and cached " + cachedCount + " products of type " + productType.getIdentifier());
            }
        }
    }

    @Override
    public Matcher getProduct(final Long id) {
        return cache.getCurrent(id);
    }

    @Override
    public Matcher getProductLocked(final Transaction transaction, final Long id) {
        return cache.getCurrentLocked(transaction, id);
    }

    public void putProduct(final Transaction transaction, final Matcher product) {
        cache.putInTransaction(transaction, product.id, product);
    }

    @Override
    public void putProductCommitted(final Transaction transaction, final Matcher product) {
        cache.putCommitted(transaction, product.id, product);
    }

    @Override
    public Matcher removeProduct(final Transaction transaction, final Long Id) {
        final Matcher p = cache.removeInTransaction(transaction, Id);
        return p;
    }

    @Override
    public JdbcDataProvider getProvider() {
        return dataProvider;
    }

    @Override
    protected TransactionalCache<Long, Matcher> getCache() {
        return cache;
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    public Set<Matcher> getMatchers(final Transaction transaction, final MatcherKey key) {
        final Collection<TransactionalEntity<Matcher>> collection = indexMatcherKey.find(key);
        if (collection == null) return Collections.emptySet(); // this should never occur, find must always return a collection
        if (collection.isEmpty()) return Collections.emptySet();
        final ImmutableSet.Builder<Matcher> builder = ImmutableSet.builder();
        for (final TransactionalEntity<Matcher> entity : collection) {
            final Matcher m = entity.getCurrentValue();
            if (m != null) builder.add(m);
        }
        return builder.build();
    }


}

final class MatcherTransactionalEntityFactory implements TransactionalEntityFactory<Matcher> {
    @Override
    public TransactionalEntity<Matcher> createCommitted(final Matcher currentValue) {
        return TransactionalEntity.createCommitted(currentValue);
    }

    @Override
    public TransactionalEntity<Matcher> createInTransaction(final Matcher newValue, final Transaction transaction) {
        return TransactionalEntity.createInTransaction(newValue, transaction);
    }
}
