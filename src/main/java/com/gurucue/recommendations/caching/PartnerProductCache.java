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

import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.ResponseException;
import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.TransactionCloseJob;
import com.gurucue.recommendations.TransactionalEntity;
import com.gurucue.recommendations.TransactionalEntityFactory;
import com.gurucue.recommendations.blender.DataSet;
import com.gurucue.recommendations.blender.TvChannelData;
import com.gurucue.recommendations.blender.VideoData;
import com.gurucue.recommendations.caching.product.AttributeIds;
import com.gurucue.recommendations.caching.product.CatalogueIdToVodActiveIndexer;
import com.gurucue.recommendations.caching.product.KeyProductTypeAndAttributes;
import com.gurucue.recommendations.caching.product.KeyProductTypeAndProductCode;
import com.gurucue.recommendations.caching.product.ProductCreator;
import com.gurucue.recommendations.caching.product.ProductTypeActiveIndexer;
import com.gurucue.recommendations.caching.product.ProductTypeAndAttributesIndexer;
import com.gurucue.recommendations.caching.product.ProductTypeAndProductCodeKeyFactory;
import com.gurucue.recommendations.caching.product.TvChannelCodeIndexer;
import com.gurucue.recommendations.caching.product.TvChannelInPackagesIndexer;
import com.gurucue.recommendations.caching.product.VodCodeInPackagesIndexer;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.product.PackageProduct;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.product.TvChannelProduct;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import com.gurucue.recommendations.entity.product.VideoProduct;
import com.gurucue.recommendations.entity.product.VodProduct;
import com.gurucue.recommendations.entity.value.AttributeValues;
import com.gurucue.recommendations.entity.value.Value;
import gnu.trove.set.TLongSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;


/**
 * Smart product cache for a partner.
 */
public final class PartnerProductCache extends ProductCache<Product> {
    private static final Logger log = LogManager.getLogger(PartnerProductCache.class);

    final long productTypeIdTvChannel;
    final long productTypeIdPackage;
    final long productTypeIdTvod;
    final long productTypeIdSvod;

    final AttributeCodes attributeCodes;
    final ProductTypeCodes productTypeCodes;

    final long partnerId;

    final TransactionalCache<Long, Product> cache;
    final JdbcDataProvider dataProvider; // used in sanity checking, to retrieve any missing products

    // the following is also among new indexes
    final CommonIndex<String, Long, Product> indexPackagesByVodCode;
    // the following is also among new indexes
    final UniqueIndex<String, Product> indexVodsByCatalogueId;
    // the following is also among new indexes
    final CommonIndex<String, Long, Product> indexPackagesByTvChannelCode;
    final TvProgrammeBeginIndex tvProgrammeBeginIndex; // the new index

    // new index definitions
    /* TODO: replace indexByTvChannelCode with indexByActiveProductTypeAndProductCode */
    @Deprecated
    final UniqueIndex<String, Product> indexByTvChannelCode;
    final TransactionalSinglekeyUniqueIndex<KeyProductTypeAndProductCode, Product> indexByProductTypeAndProductCode;
    final CommonIndex<Long, Long, Product> indexByActiveProductType;
    //--------------------------------------------------

    // internal cache, must be invalidated after every package and SVOD/TVOD modification
    private final AtomicContainer<Future<Map<String, List<PackageProduct>>>> currentCatalogueIdToPackagesMapping = new AtomicContainer<>();
    // internal cache, must be invalidated after every package and TV-channel modification
    private final AtomicContainer<Future<Map<String, TvChannelData>>> currentTvChannelCodeToPackageMapping = new AtomicContainer<>();

    /**
     * Constructs and populates a product cache. The provided provider must be
     * a physical provider, i.e. not the caching provider where this cache
     * belongs to.
     * @param partnerId the partner ID for which this cache caches products
     * @param provider the underlying physical data provider
     */
    public PartnerProductCache(final long partnerId, final JdbcDataProvider provider) {
        this.partnerId = partnerId;
        this.dataProvider = provider;
        this.attributeCodes = provider.getAttributeCodes();
        this.productTypeCodes = provider.getProductTypeCodes();
        final String threadId = "[" + Thread.currentThread().getId() + "] ";
        final long startNano = System.nanoTime();
        log.info(threadId + "Creating product cache");
        try {
            JdbcDataLink link = provider.newJdbcDataLink();
            try {
                productTypeIdTvChannel = productTypeCodes.idForTvChannel;
                productTypeIdPackage = productTypeCodes.idForPackage;
                productTypeIdTvod = productTypeCodes.idForTvod;
                productTypeIdSvod = productTypeCodes.idForSvod;

                // build indices
                log.info(threadId + "Creating cache indices");
                indexPackagesByVodCode = CommonIndex.newHashIndex(new VodCodeInPackagesIndexer(productTypeIdPackage));
                indexVodsByCatalogueId = UniqueIndex.newHashIndex(new CatalogueIdToVodActiveIndexer(productTypeCodes.idForSvod, productTypeCodes.idForTvod));
                indexPackagesByTvChannelCode = CommonIndex.newHashIndex(new TvChannelInPackagesIndexer(productTypeIdPackage));

                // new indices
                indexByTvChannelCode = UniqueIndex.newHashIndex(new TvChannelCodeIndexer(productTypeCodes.idForTvChannel));
                indexByProductTypeAndProductCode = TransactionalSinglekeyUniqueIndex.newHashIndex(new ProductTypeAndProductCodeKeyFactory());
                indexByActiveProductType = CommonIndex.newHashIndex(new ProductTypeActiveIndexer());
                tvProgrammeBeginIndex = new TvProgrammeBeginIndex(this);

                // define cache
                cache = new TransactionalCache<>(
                        new PartnerProductTransactionalEntityFactory(this, productTypeCodes.idForTvProgramme),
                        1000000,
                        new Index[]{indexPackagesByVodCode, indexVodsByCatalogueId, indexPackagesByTvChannelCode, indexByTvChannelCode, indexByActiveProductType, tvProgrammeBeginIndex},
                        new TransactionalUniqueIndex[]{indexByProductTypeAndProductCode}
                );

                // fill cache (read data into memory)
                fillCache(link);
            }
            finally {
                link.close();
            }

        }
        catch (SQLException|ResponseException e) {
            throw new DatabaseException("Failed to create a product cache for partner " + partnerId + " because of a database error: " + e.toString(), e);
        }

        final long endNano = System.nanoTime();
        log.info(threadId + "Product cache created and initialized in " + (endNano - startNano) + " ns with " + cache.size() + " elements");
    }

    private void fillCache(final JdbcDataLink link) throws SQLException, ResponseException {
        final String threadId = "[" + Thread.currentThread().getId() + "] ";
        log.info(threadId + "Filling product cache for partner " + partnerId);
        final StringBuilder logBuilder = new StringBuilder(16384);
        final Map<String, TvChannelProduct> tvChannels = new HashMap<>();

        // define parameters for "simple" products retrieval: read everything except for tv-programmes
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
                        return new VideoProduct(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.interactive;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        // use a vanilla product for interactive products
                        return new Product(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder));
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.svod;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        return new VodProduct(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.tvod;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        return new VodProduct(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.tvChannel;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        final TvChannelProduct tvChannel = new TvChannelProduct(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                        tvChannels.put(tvChannel.partnerProductCode, tvChannel);
                        return tvChannel;
                    }
                },
                new ProductCreator() {
                    private final ProductType productType = productTypeCodes.package_;
                    private final long productTypeId = productType.getId();
                    @Override
                    public ProductType getProductType() {
                        return productType;
                    }
                    @Override
                    public Product create(final long id, final String partnerProductCode, final String jsonAttributes, final String jsonRelated, final Timestamp added, final Timestamp modified, final Timestamp deleted) throws ResponseException {
                        return new PackageProduct(id, productTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    }
                }
        };

        final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

        try (final PreparedStatement st = link.prepareStatement("select p.id, p.partner_product_code, p.added, p.modified, p.deleted, p.attributes::text, p.related::text from product p where p.partner_id = ? and p.product_type_id = ? and p.deleted is null", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(2000);
            for (int i = 0; i < productCreators.length; i++) {
                final ProductCreator creator = productCreators[i];
                final ProductType productType = creator.getProductType();
                log.info(threadId + "Reading and caching products of type " + productType.getIdentifier());
                st.setLong(1, partnerId);
                st.setLong(2, productType.getId());
                int count = 0;
                try (final ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        count++;
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
                        cache.putFast(id, creator.create(id, partnerProductCode, jsonAttributes, jsonRelated, added, modified, deleted));
                    }
                }
                log.info(threadId + "Read " + count + " products of type " + productType.getIdentifier());
            }
        }

        // now read tv-programmes, but only those that are in the catchup or in the future
        // find maximum catchup-hours
        long maxCatchupHours = 0L;
        for (final TvChannelProduct entity : tvChannels.values()) {
            final long catchupHours;
            catchupHours = entity.catchupHours;
            if (catchupHours > maxCatchupHours) maxCatchupHours = catchupHours;
        }
        final long maxCatchupMillis = maxCatchupHours * 3600000L;
        final long tvProgrammeTypeId = productTypeCodes.idForTvProgramme;
        final long nowMillis = Timer.currentTimeMillis();
        final long reserveCatchupMillis = TransactionalTvProgrammeEntity.TV_PROGRAMME_CACHING_RESERVE_MILLIS;
        final long minDbBeginTimeSeconds = (nowMillis - maxCatchupMillis - reserveCatchupMillis) / 1000L;
        log.debug("Using max catchup hours = " + maxCatchupHours + ", max catchup millis = " + maxCatchupMillis + ", reserve catchup millis = " + reserveCatchupMillis + ", reading all tv-programmes from the database since " + minDbBeginTimeSeconds + " s");
/*
select id, partner_product_code, attributes::text, related::text
from product
where partner_id = 6 and product_type_id = 3 and deleted is null
and cast(attributes->>'begin-time' as bigint) >= 1397745240
order by attributes#>>'{tv-channel,0}'
*/
        try (final PreparedStatement st = link.prepareStatement(
                "select id, partner_product_code, added, modified, deleted, attributes::text, related::text " +
                "from product " +
                "where partner_id = ? and product_type_id = ? and deleted is null " +
                "and cast(attributes->>'begin-time' as bigint) >= ? " +
                "order by attributes#>>'{tv-channel,0}'", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(2000);
            st.setLong(1, partnerId);
            st.setLong(2, tvProgrammeTypeId);
            st.setLong(3, minDbBeginTimeSeconds);
            int count = 0;
            int cached = 0;
            try (final ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    count++;
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
                    final TvProgrammeProduct tvProgramme = new TvProgrammeProduct(id, tvProgrammeTypeId, partnerId, partnerProductCode, added, modified, deleted, AttributeValues.fromJson(jsonAttributes, dataProvider, logBuilder),  AttributeValues.fromJson(jsonRelated, dataProvider, logBuilder), dataProvider);
                    if ((tvProgramme.tvChannelCodes == null) || (tvProgramme.tvChannelCodes.length == 0)) {
                        // the tv-programme belongs to no tv-channel, therefore it cannot be played, and not recommended
                        continue;
                    }
                    // discover the length of catch-up
                    long catchupMillis = 0L;
                    for (final String tvChannelCode : tvProgramme.tvChannelCodes) {
                        final TvChannelProduct tvChannel = tvChannels.get(tvChannelCode);
                        // assume the worst: the missing tv-channel has the maximum length catch-up
                        if (tvChannel == null) catchupMillis = maxCatchupMillis;
                        else {
                            if (tvChannel.catchupMillis > catchupMillis) catchupMillis = tvChannel.catchupMillis;
                        }
                    }
                    catchupMillis += reserveCatchupMillis;
                    // cache it if it's within the catch-up reach or in the future
                    if (tvProgramme.beginTimeMillis >= (nowMillis - catchupMillis)) {
                        cache.putFast(id, tvProgramme);
                        cached++;
                    }
                }
            }
            log.info(threadId + "Read " + count + " products of type " + productTypeCodes.tvProgramme.getIdentifier() + ", cached " + cached);
        }
    }

    public void destroy() {
        cache.destroy(); // the cache also destroys all its indexes
        log.info("Product cache destroyed");
    }

    @Override
    public JdbcDataProvider getProvider() {
        return dataProvider;
    }

    @Override
    protected TransactionalCache<Long, Product> getCache() {
        return cache;
    }

    @Override
    protected Logger getLog() {
        return log;
    }

    // basic low-level access methods

    @Override
    public void putProductCommitted(final Transaction transaction, final Product product) {
        cache.putCommitted(transaction, product.id, product);
        invalidateCachesIfNecessary(product);
    }

    public void putProduct(final Transaction transaction, final Product product) {
        cache.putInTransaction(transaction, product.id, product);
        transaction.onTransactionClose(new InvalidateCachesOnCommitJob(this, product));
    }

    protected void invalidateCachesIfNecessary(final Product product) {
        // invalidate package mapping caches if necessary
        final long productTypeId = product.productTypeId;
        if (productTypeId == productTypeIdPackage) {
            currentCatalogueIdToPackagesMapping.set(null);
            currentTvChannelCodeToPackageMapping.set(null);
        }
        else if (productTypeId == productTypeIdTvChannel) {
            currentTvChannelCodeToPackageMapping.set(null);
        }
        else if ((productTypeId == productTypeIdSvod) || (productTypeId == productTypeIdTvod)) {
            currentCatalogueIdToPackagesMapping.set(null);
        }
    }

    public Product removeProduct(final Transaction transaction, final long productTypeId, final String partnerProductCode) {
        final KeyProductTypeAndProductCode key = new KeyProductTypeAndProductCode(productTypeId, partnerProductCode);
        final TransactionalEntity<Product> entity = indexByProductTypeAndProductCode.findAndLock(transaction, key, true);
        if (entity == null) return null;
        final Product p = entity.getCurrentValue();
        if (p == null) return null;
        cache.removeExistingInTransaction(transaction, p.id, entity);
        transaction.onTransactionClose(new InvalidateCachesOnCommitJob(this, p));
        return p;
    }

    @Override
    public Product removeProduct(final Transaction transaction, final Long id) {
        final Product p = cache.removeInTransaction(transaction, id);
        if (p == null) return null;
        transaction.onTransactionClose(new InvalidateCachesOnCommitJob(this, p));
        return p;
    }

    public Product removeProductCommitted(final Transaction transaction, final Long id) {
        return cache.removeCommitted(transaction, id);
    }

    public void removeProducts(final Transaction transaction, final Collection<Long> IDs) {
        final List<Product> removedProducts = cache.removeInTransaction(transaction, IDs);
        if (removedProducts.isEmpty()) return;
        transaction.onTransactionClose(new InvalidateCachesOnCommitMultiJob(this, removedProducts));
    }

    @Override
    public Product getProduct(final Long id) {
        return cache.getCurrent(id);
    }

    @Override
    public Product getProductLocked(final Transaction transaction, final Long id) {
        return cache.getCurrentLocked(transaction, id);
    }

    public Product getProduct(final Transaction transaction, final long productTypeId, final String partnerProductCode, final boolean locked) {
        final KeyProductTypeAndProductCode key = new KeyProductTypeAndProductCode(productTypeId, partnerProductCode);
        final TransactionalEntity<Product> entity;
        if (locked) entity = indexByProductTypeAndProductCode.findAndLock(transaction, key, false);
        else entity = indexByProductTypeAndProductCode.find(key);
        if (entity == null) return null;
        return entity.getCurrentValue();
    }

    private final ConcurrentMap<AttributeIds, Future<CommonIndex<KeyProductTypeAndAttributes, Long, Product>>> variantIndices = new ConcurrentHashMap<>();

    private CommonIndex<KeyProductTypeAndAttributes, Long, Product> getVariantIndex(final AttributeIds attributeIds) {
        for (;;) {
            Future<CommonIndex<KeyProductTypeAndAttributes, Long, Product>> f = variantIndices.get(attributeIds);
            if (f == null) {
                FutureTask<CommonIndex<KeyProductTypeAndAttributes, Long, Product>> ft = new FutureTask<>(new VariantIndexCreator(cache, attributeIds, attributeCodes));
                f = variantIndices.putIfAbsent(attributeIds, ft);
                if (f == null) {
                    log.info("Creating new variant index: " + attributeIds.toString());
                    f = ft;
                    ft.run();
                }
            }
            try {
                return f.get();
            }
            catch (CancellationException e) {
                variantIndices.remove(attributeIds, f);
            }
            catch (ExecutionException e) {
                variantIndices.remove(attributeIds, f);
                throw new IllegalStateException("Variant index computation failed: " + e.toString(), e);
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("Interrupted while waiting for variant index: " + e.toString(), e);
            }
        }
    }

    public List<Product> findProductsByVariantIndex(final ProductType productType, final Map<Attribute, Value> productAttributes) {
        if (log.isDebugEnabled()) {
            final StringBuilder sb = new StringBuilder(150 + (productAttributes.size() * 40)); // guesstimate
            sb.append("findProductsByVariantIndex(): using variant index and values: productType=");
            sb.append(productType.getIdentifier());
            if (productAttributes.size() == 0) {
                sb.append(", [no attribute values]");
            }
            else {
                for (final Map.Entry<Attribute, Value> pa : productAttributes.entrySet()) {
                    sb.append(", ");
                    sb.append(pa.getKey().getIdentifier());
                    sb.append("=");
                    pa.getValue().toJson(sb);
                }
            }
            log.debug(sb.toString());
        }

        final AttributeIds ids = new AttributeIds(productAttributes.keySet());
        final List<KeyProductTypeAndAttributes> keys = KeyProductTypeAndAttributes.createKeys(productType.getId(), ids, productAttributes, attributeCodes);
        final List<Product> result;
        final int keySize = keys.size();
        if (keySize == 0) {
            result = Collections.emptyList();
        }
        else if (keySize == 1) {
            // optimization step: majority of cases will result in only one key
            result = new ArrayList<>(getVariantIndex(ids).find(keys.get(0)));
        }
        else {
            // collect products for all keys through mapping by product IDs, to remove any duplicates
            final Map<Long, Product> resultMap = new HashMap<>();
            final CommonIndex<KeyProductTypeAndAttributes, Long, Product> index = getVariantIndex(ids);
            for (final KeyProductTypeAndAttributes key : keys) {
                for (final Product p : index.find(key)) {
                    resultMap.put(p.id, p);
                }
            }
            result = new ArrayList<>(resultMap.values());
        }

        if (log.isDebugEnabled()) {
            if (result.size() == 0) log.debug("findProductsByVariantIndex(): no results");
            else {
                final StringBuilder sb = new StringBuilder(60 + (result.size() * 11)); // guesstimate
                sb.append("findProductsByVariantIndex(): found ");
                sb.append(result.size());
                sb.append(" results: ");
                final Iterator<Product> it = result.iterator();
                Product p = it.next();
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

    /**
     * The following method exists for backwards compatibility in ProductManagerCached.tvProgrammesInIntervalForPartner().
     *
     * @param recommendTimeMillis the time at which the recommendation request is being served
     * @param negativeOffsetMillis the range in milliseconds into the past, relative to recommendTimeMillis, in which content is to be collected
     * @param positiveOffsetMillis the range in milliseconds into the future, relative to recommendTimeMillis, in which content is to be collected
     * @param subscriptionPackageIds the set of IDs of packages to which the requester is subscribed
     * @param outputList the list into which to add the content
     */
    public void findTvProgrammesBetweenTimestamps(
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final List<VideoData> outputList
    ) {
        tvProgrammeBeginIndex.reachableTvProgrammesInRange(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, subscriptionPackageIds, outputList);
    }

    public void reachableTvProgrammesInRange(
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        tvProgrammeBeginIndex.reachableTvProgrammesInRange(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, subscriptionPackageIds, outputBuilder);
    }

    public TvProgrammeProduct findTvProgrammeAtTime(final String tvChannelCode, final long timeMillis) {
        return tvProgrammeBeginIndex.tvProgrammeAt(tvChannelCode, timeMillis);
    }

    public TvProgrammeProduct findFirstTvProgrammeAfterTime(final String tvChannelCode, final long timeMillis) {
        return tvProgrammeBeginIndex.firstTvProgrammeAfter(tvChannelCode, timeMillis);
    }

    public Collection<Product> findProductEntitiesOfProductType(final Long productTypeId) {
        return indexByActiveProductType.find(productTypeId);
    }

    public SortedSet<TvProgrammeProduct> findOverlappingTvProgrammes(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis) {
        return tvProgrammeBeginIndex.tvProgrammesOverlapping(tvChannelCode, beginTimeMillis, endTimeMillis);
    }

    public void forEachOverlappingTvProgramme(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
        tvProgrammeBeginIndex.forEachOverlappingTvProgramme(tvChannelCode, beginTimeMillis, endTimeMillis, consumer);
    }

    public VodProduct findVodCatalogueByCatalogueId(final String catalogueId) {
        final Product product = indexVodsByCatalogueId.find(catalogueId);
        if (product == null) return null;
        try {
            return (VodProduct) product;
        }
        catch (ClassCastException e) {
            log.error("indexVodsByCatalogueId returned an entity having a " + product.getClass().getCanonicalName() + " instance instead of VodProduct");
            return null;
        }
    }

    public Map<String, List<PackageProduct>> currentCatalogueIdToPackagesMapping() {
        Future<Map<String, List<PackageProduct>>> f = currentCatalogueIdToPackagesMapping.get();
        if (f == null) {
            final FutureTask<Map<String, List<PackageProduct>>> ft = new FutureTask<>(new CreateCatalogueIdToPackagesMapping(this));
            f = currentCatalogueIdToPackagesMapping.compareAndSet(null, ft);
            if (f == ft) {
                ft.run();
            }
        }
        try {
            return f.get();
        } catch (InterruptedException e) {
            final String message = "Interrupted while computing current catalogue-id to packages mapping: " + e.toString();
            log.error(message, e);
            throw new DatabaseException(message, e);
        } catch (ExecutionException e) {
            final String message = "Execution exception while computing current catalogue-id to packages mapping: " + e.toString();
            log.error(message, e);
            throw new DatabaseException(message, e);
        }
    }

    private static final class CreateCatalogueIdToPackagesMapping implements Callable<Map<String, List<PackageProduct>>> {
        private static final Logger logger = LogManager.getLogger(CreateCatalogueIdToPackagesMapping.class);

        private final PartnerProductCache cache;

        CreateCatalogueIdToPackagesMapping(final PartnerProductCache cache) {
            this.cache = cache;
        }

        @Override
        public Map<String, List<PackageProduct>> call() throws Exception {
            final Map<String, Product> vodMap = cache.indexVodsByCatalogueId.snapshot();
            final Map<String, List<PackageProduct>> result = new HashMap<>(vodMap.size());
            vodMap.forEach((final String catalogueId, final Product vod) -> {
                final Collection<Product> packageProducts = cache.indexPackagesByVodCode.find(vod.partnerProductCode);
                final List<PackageProduct> packages = new ArrayList<>(packageProducts.size());
                packageProducts.forEach((final Product packageProduct) -> {
                    try {
                        packages.add((PackageProduct) packageProduct);
                    }
                    catch (ClassCastException e) {
                        logger.error("The index indexPackagesByVodCode contains an instance that is something else than a PackageProduct instance: " + packageProduct.getClass().getCanonicalName(), e);
                    }
                });
                if (packages.isEmpty()) return; // skip unsubscribable vods
                result.put(catalogueId, packages);
            });
            return result;
        }
    }

    // obtain a list of packages that contain the given tv-channel
    public Map<String, TvChannelData> currentTvChannelCodeToPackageMapping() {
        Future<Map<String, TvChannelData>> f = currentTvChannelCodeToPackageMapping.get();
        if (f == null) {
            final FutureTask<Map<String, TvChannelData>> ft = new FutureTask<>(new CreateTvChannelCodeToPackagesMapping(this));
            f = currentTvChannelCodeToPackageMapping.compareAndSet(null, ft);
            if (f == ft) {
                ft.run();
            }
        }
        try {
            return f.get();
        } catch (InterruptedException e) {
            final String message = "Interrupted while computing current tv-channel to packages mapping: " + e.toString();
            log.error(message, e);
            throw new DatabaseException(message, e);
        } catch (ExecutionException e) {
            final String message = "Execution exception while computing current tv-channel to packages mapping: " + e.toString();
            log.error(message, e);
            throw new DatabaseException(message, e);
        }
    }

    private static final class CreateTvChannelCodeToPackagesMapping implements Callable<Map<String, TvChannelData>> {
        private static final Logger logger = LogManager.getLogger(CreateTvChannelCodeToPackagesMapping.class);

        private final PartnerProductCache cache;

        CreateTvChannelCodeToPackagesMapping(final PartnerProductCache cache) {
            this.cache = cache;
        }

        @Override
        public Map<String, TvChannelData> call() throws Exception {
            final Map<String, Collection<Product>> snapshot = cache.indexPackagesByTvChannelCode.snapshot();
            final Map<String, TvChannelData> result = new HashMap<>(snapshot.size());
            final StringBuilder builderNull = new StringBuilder(512);
            final StringBuilder builderEmpty = new StringBuilder(2048);
            snapshot.forEach((final String tvChannelCode, final Collection<Product> src) -> {
                final TvChannelProduct tvChannel = cache.getTvChannelByProductCode(tvChannelCode);
                if (tvChannel == null) {
                    if (builderNull.length() == 0) builderNull.append(tvChannelCode);
                    else builderNull.append(", ").append(tvChannelCode);
                    return;
                }
                final List<PackageProduct> packages = new LinkedList<>();
                src.forEach((final Product product) -> {
                    if (product == null) return;
                    try {
                        packages.add((PackageProduct) product);
                    }
                    catch (ClassCastException e) {
                        logger.error("The index indexPackagesByTvChannelCode contains an instance that is something else than a PackageProduct instance: " + product.getClass().getCanonicalName(), e);
                    }
                });
                if (packages.isEmpty()) {
                    // skip unsubscribable tv-channels
                    if (builderEmpty.length() == 0) builderEmpty.append(tvChannelCode);
                    else builderEmpty.append(", ").append(tvChannelCode);
                    return;
                }
                result.put(tvChannelCode, new TvChannelData(tvChannel, packages, false));
            });
            if ((builderEmpty.length() > 0) || (builderNull.length() > 0)) {
                final StringBuilder builderLog = new StringBuilder(builderEmpty.length() + builderNull.length() + 1024);
                builderLog.append("Errors during assembling TV-channel subscription data:");
                if (builderNull.length() > 0) builderLog.append("\n  no TV-channel product defined for partner codes: ").append(builderNull);
                if (builderEmpty.length() > 0) builderLog.append("\n  no subscription packages defined for TV-channels: ").append(builderEmpty);
                log.error(builderLog.toString());
            }
            return result;
        }
    }

    public TvChannelProduct getTvChannelByProductCode(final String partnerProductCode) {
        final TransactionalEntity<Product> entity = indexByProductTypeAndProductCode.find(new KeyProductTypeAndProductCode(productTypeIdTvChannel, partnerProductCode));
        if (entity == null) return null;
        final Product p = entity.getCurrentValue();
        try {
            return (TvChannelProduct)p;
        }
        catch (ClassCastException e) {
            log.error("getTvChannelByProductCode(): not a TvChannelProduct (id=" + p.id + ", partner product code=" + p.partnerProductCode + "): " + e.toString(), e);
            return null;
        }
    }


    static final class VariantIndexCreator implements Callable<CommonIndex<KeyProductTypeAndAttributes, Long, Product>> {
        private final AttributeIds attributeIds;
        private final TransactionalCache<Long, Product> cache;
        private final AttributeCodes attributeCodes;

        VariantIndexCreator(final TransactionalCache<Long, Product> cache, final AttributeIds attributeIds, final AttributeCodes attributeCodes) {
            this.cache = cache;
            this.attributeIds = attributeIds;
            this.attributeCodes = attributeCodes;
        }

        @Override
        public CommonIndex<KeyProductTypeAndAttributes, Long, Product> call() throws Exception {
            final CommonIndex<KeyProductTypeAndAttributes, Long, Product> index = CommonIndex.newHashIndex(new ProductTypeAndAttributesIndexer(attributeIds, attributeCodes));
            cache.addIndex(index);
            return index;
        }
    }

    static final class AtomicContainer<V> {
        private V value;

        public synchronized void set(final V newValue) {
            value = newValue;
        }

        public synchronized V get() {
            return value;
        }

        public synchronized V compareAndSet(final V expectedValue, final V newValue) {
            if ((expectedValue == value) || ((expectedValue != null) && expectedValue.equals(value))) {
                return value = newValue;
            }
            return value;
        }
    }
}

final class PartnerProductTransactionalEntityFactory implements TransactionalEntityFactory<Product> {
    private final PartnerProductCache cache;
    private final long tvProgrammeProductTypeId;

    public PartnerProductTransactionalEntityFactory(final PartnerProductCache cache, final long tvProgrammeProductTypeId) {
        this.cache = cache;
        this.tvProgrammeProductTypeId = tvProgrammeProductTypeId;
    }

    @Override
    public TransactionalEntity<Product> createCommitted(final Product currentValue) {
        if ((currentValue != null) && (currentValue.productTypeId == tvProgrammeProductTypeId)) return TransactionalTvProgrammeEntity.createCommitted(cache, (TvProgrammeProduct)currentValue);
        return TransactionalEntity.createCommitted(currentValue);
    }

    @Override
    public TransactionalEntity<Product> createInTransaction(final Product newValue, final Transaction transaction) {
        if ((newValue != null) && (newValue.productTypeId == tvProgrammeProductTypeId)) return TransactionalTvProgrammeEntity.createInTransaction(cache, (TvProgrammeProduct)newValue, transaction);
        return TransactionalEntity.createInTransaction(newValue, transaction);
    }
}

final class InvalidateCachesOnCommitJob implements TransactionCloseJob {
    private final PartnerProductCache cache;
    private final Product product;

    public InvalidateCachesOnCommitJob(final PartnerProductCache cache, final Product product) {
        this.cache = cache;
        this.product = product;
    }

    @Override
    public void commit() {
        cache.invalidateCachesIfNecessary(product);
    }

    @Override
    public void rollback() {
        // no-op
    }
}

final class InvalidateCachesOnCommitMultiJob implements TransactionCloseJob {
    private final PartnerProductCache cache;
    private final List<Product> products;

    public InvalidateCachesOnCommitMultiJob(final PartnerProductCache cache, final List<Product> products) {
        this.cache = cache;
        this.products = products;
    }

    @Override
    public void commit() {
        for (final Product product : products) {
            cache.invalidateCachesIfNecessary(product);
        }
    }

    @Override
    public void rollback() {
        // no-op
    }
}
