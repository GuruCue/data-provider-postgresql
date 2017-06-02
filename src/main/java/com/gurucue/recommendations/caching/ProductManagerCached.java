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

import com.google.common.collect.ImmutableList;
import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.ProcessingException;
import com.gurucue.recommendations.ResponseStatus;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.TransactionCloseJob;
import com.gurucue.recommendations.blender.DataSet;
import com.gurucue.recommendations.blender.TvChannelData;
import com.gurucue.recommendations.blender.VideoData;
import com.gurucue.recommendations.caching.product.PartnerProductCacheProcedure;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.product.GeneralVideoProduct;
import com.gurucue.recommendations.entity.product.Matcher;
import com.gurucue.recommendations.entity.product.MatcherKey;
import com.gurucue.recommendations.entity.product.PackageProduct;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.TvChannelProduct;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import com.gurucue.recommendations.entity.product.VideoProduct;
import com.gurucue.recommendations.entity.product.VodProduct;
import com.gurucue.recommendations.entity.value.TimestampIntervalValue;
import com.gurucue.recommendations.entity.value.Value;
import com.gurucue.recommendations.entitymanager.ProductManager;
import com.gurucue.recommendations.entitymanager.TvChannelInfo;
import com.gurucue.recommendations.entitymanager.TvProgrammeInfo;
import com.gurucue.recommendations.entitymanager.VideoInfo;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.set.TLongSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.function.Consumer;

public final class ProductManagerCached implements ProductManager {
    private static final Logger log = LogManager.getLogger(ProductManagerCached.class);

    private final CachedJdbcDataLink dataLink;
    private ProductManager physicalManager = null;

    public ProductManagerCached(final CachedJdbcDataLink dataLink) {
        this.dataLink = dataLink;
    }

    private ProductManager getPhysicalManager() {
        // lazy init: we don't need physical provider for cached data
        if (physicalManager == null) physicalManager = dataLink.getPhysicalLink().getProductManager();
        return physicalManager;
    }

    private PartnerProductCache getPartnerProductCache(final Long partnerId) {
        return dataLink.provider.getProductCacheForPartner(partnerId);
    }

    @Override
    public Product getProductByPartnerAndTypeAndCode(final Transaction transaction, final Partner partner, final ProductType productType, final String productCode, boolean locked) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final Product cachedProduct = cache.getProduct(transaction, productType.getId(), productCode, locked);
        if (cachedProduct != null) {
            return cachedProduct;
        }
        // try to retrieve it from the original manager
        final Product dbProduct = getPhysicalManager().getProductByPartnerAndTypeAndCode(transaction, partner, productType, productCode, locked);
        if (dbProduct == null) return null;
        // now cache it; this does not interfere with the previously locked index key, which was done if locked==true: it will simply be unlocked and forgotten upon transaction commit/rollback
        if (dbProduct.deleted == null) {
            log.warn("Un-cached product found, caching it: " + dbProduct.toString());
            // TODO: also seek and cache the matcher
            cache.putProductCommitted(transaction, dbProduct);
        }
        else {
            log.warn("Returning an un-cached product, not caching it since it's deleted: " + dbProduct.toString());
        }
        return dbProduct;
    }

    // TODO: implement transaction lock in caching layer
    @Override
    public Product getById(final Transaction transaction, final Partner partner, final long productId, boolean locked) {
        if (locked) {
            // a partner hint was given, use it
            if (partner != null) {
                if (partner.getId().longValue() == Partner.PARTNER_ZERO_ID) {
                    final InternalProductCache cache = dataLink.provider.getProductMatcherCache();
                    final Product cachedProduct = cache.getProductLocked(transaction, productId);
                    if (cachedProduct != null) return cachedProduct;
                }
                else {
                    final PartnerProductCache cache = getPartnerProductCache(partner.getId());
                    final Product cachedProduct = cache.getProductLocked(transaction, productId);
                    if (cachedProduct != null) return cachedProduct;
                }
            }

            // try to find it over all caches; TODO: avoid re-check on hinted partner, if provided
            // this will lock the ID with all partner caches that don't have the product, but this should not be a problem as only one cache can have the product
            final ProductCacheLockedSeeker seeker = new ProductCacheLockedSeeker(transaction, productId);
            dataLink.provider.withEachPartnerProductCache(seeker);

            if (seeker.foundProduct != null) return seeker.foundProduct;

            // try to find it in the internal cache
            final Product cachedProduct = dataLink.provider.getProductMatcherCache().getProductLocked(transaction, productId);
            if (cachedProduct != null) return cachedProduct;
        }
        else {
            // a partner hint was given, use it
            if (partner != null) {
                if (partner.getId().longValue() == Partner.PARTNER_ZERO_ID) {
                    final InternalProductCache cache = dataLink.provider.getProductMatcherCache();
                    final Product cachedProduct = cache.getProduct(productId);
                    if (cachedProduct != null) return cachedProduct;
                }
                else {
                    final PartnerProductCache cache = getPartnerProductCache(partner.getId());
                    final Product cachedProduct = cache.getProduct(productId);
                    if (cachedProduct != null) return cachedProduct;
                }
            }

            // try to find it over all caches; TODO: avoid re-check on hinted partner, if provided
            final ProductCacheSeeker seeker = new ProductCacheSeeker(productId);
            dataLink.provider.withEachPartnerProductCache(seeker);

            if (seeker.foundProduct != null) return seeker.foundProduct;

            // try to find it in the internal cache
            final Product cachedProduct = dataLink.provider.getProductMatcherCache().getProduct(productId);
            if (cachedProduct != null) return cachedProduct;
        }

        // try to retrieve it from the original manager
        final Product dbProduct = getPhysicalManager().getById(transaction, partner, productId, locked);
        if (dbProduct == null) return null;

        if (dbProduct.deleted != null) {
            log.warn("Returning an un-cached product, not caching it since it's deleted: " + dbProduct.toString());
            return dbProduct;
        }

        // cache the product
        log.warn("Un-cached product found, caching it: " + dbProduct.toString());
        if (dbProduct.partnerId == Partner.PARTNER_ZERO_ID) {
            try {
                dataLink.provider.getProductMatcherCache().putProductCommitted(transaction, (Matcher) dbProduct);
            }
            catch (ClassCastException e) {
                log.error("Cannot cache a product of partner zero: it is not a Matcher descendat: id=" + dbProduct.id + ", class=" + dbProduct.getClass().getCanonicalName(), e);
            }
        }
        else {
            final PartnerProductCache cache = getPartnerProductCache(dbProduct.partnerId);
            // TODO: also seek and cache the matcher
            if (cache == null) {
                log.error("There is no partner product cache for partner " + dbProduct.partnerId + ": product " + dbProduct.id);
            } else {
                cache.putProductCommitted(transaction, dbProduct);
            }
        }
        return dbProduct;
    }

    @Override
    public void deleteByPartnerAndTypeAndCode(final Transaction transaction, final Partner partner, final ProductType productType, final String productCode) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        getPhysicalManager().deleteByPartnerAndTypeAndCode(transaction, partner, productType, productCode);
        cache.removeProduct(transaction, productType.getId(), productCode);
    }

    @Override
    public Product delete(final Transaction transaction, final long id) {
        final Product p = getPhysicalManager().delete(transaction, id);
        if (p == null) return null;
        if (p.partnerId == Partner.PARTNER_ZERO_ID) {
            dataLink.provider.getProductMatcherCache().removeProduct(transaction, p.id);
        }
        else {
            final PartnerProductCache cache = dataLink.provider.getProductCacheForPartner(p.partnerId);
            if (cache == null) {
                log.error("delete(): There is no partner cache for partner_id=" + p.partnerId);
            }
            else {
                cache.removeProduct(transaction, p.id);
            }
        }
        return p;
    }

    @Override
    public Product save(final Transaction transaction, final Product product) {
        final Product p = getPhysicalManager().save(transaction, product);
        if (p.partnerId == Partner.PARTNER_ZERO_ID) {
            if (p instanceof Matcher) {
                dataLink.provider.getProductMatcherCache().putProduct(transaction, (Matcher)p);
            }
            else {
                throw new ProcessingException(ResponseStatus.UNKNOWN_ERROR, "Saving to internal cache requires a Matcher instance, but it was given: " + p.getClass().getCanonicalName());
            }
        }
        else {
            final PartnerProductCache cache = getPartnerProductCache(p.partnerId);
            cache.putProduct(transaction, p);
        }
        return p;
    }

    @Override
    public List<Product> findProductsHavingAttributes(final Transaction transaction, final ProductType productType, final Partner partner, final Map<Attribute, Value> attributeValues) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        return cache.findProductsByVariantIndex(productType, attributeValues);
    }

    // TODO: do not remove Product instances that overlap, instead just remove the tv-channel code from their attributes,
    // TODO: so we gain the proper multi-tv-channel support for tv-programmes
    // TODO: --- create a caching index to use for overlap deletion, instead of using a complex SQL ---
    @Override
    public List<Product> removeTvProgrammesOverlappingInterval(final Transaction transaction, final TvProgrammeProduct tvProgramme/*final Partner partner, final String tvChannelCode, final long beginTime, final long endTime, final Product exception*/) {
        // use the cache to find overlaps, and then remove them
        if (tvProgramme.endTimeMillis < tvProgramme.beginTimeMillis) throw new IllegalArgumentException("Illegal timestamps for the given TV-programme: endTime < beginTime, in: " + tvProgramme.toString());
        final PartnerProductCache cache = getPartnerProductCache(tvProgramme.partnerId);
        final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
        final ImmutableList.Builder<Product> removedProductsBuilder = ImmutableList.builder();
        final Long beginTimeMillis = tvProgramme.beginTimeMillis;
        final Long endTimeMillis = tvProgramme.endTimeMillis;
        final long newId = tvProgramme.id;
        final ProductManager pm = getPhysicalManager();
        for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
            cache.findOverlappingTvProgrammes(tvChannelCodes[i], beginTimeMillis, endTimeMillis).forEach((final TvProgrammeProduct p) -> {
                if (p.id != newId) { // do not delete the new TV-programme
                    removedProductsBuilder.add(p); // remember for later, when the transaction is committed
                    pm.delete(transaction, p.id); // delete physically, not just in cache
                }
            });
        }
        final ImmutableList<Product> removedProducts = removedProductsBuilder.build();
        if (!removedProducts.isEmpty()) {
            transaction.onTransactionClose(new TransactionCloseJob() {
                @Override
                public void commit() {
                    removedProducts.forEach((final Product p) -> cache.removeProductCommitted(transaction, p.id));
                }

                @Override
                public void rollback() {

                }
            });
        }
        return removedProducts;
    }

    public void forEachOverlappingTvProgramme(final Transaction transaction, final Partner partner, final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        cache.forEachOverlappingTvProgramme(tvChannelCode, beginTimeMillis, endTimeMillis, consumer);
    }

    public void replaceTvProgrammes(final Transaction transaction, final Collection<TvProgrammeProduct> replacements) {
        // slice them all up according to partners and tv-channels
        final TLongObjectMap<HashMap<String, TreeSet<TvProgrammeProduct>>> byPartnerId = new TLongObjectHashMap<>();
        long cachedPartnerId = -1L;
        HashMap<String, TreeSet<TvProgrammeProduct>> cachedByChannel = null;
        String cachedTvChannel = null;
        TreeSet<TvProgrammeProduct> cachedSet = null;
        for (final TvProgrammeProduct tvProgramme : replacements) {
            final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
            if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                log.error("replaceTvProgrammes(): the tv-programme has no tv-channel set: ID=" + tvProgramme.id + ", partner_id=" + tvProgramme.partnerId + ", partner_product_code=" + tvProgramme.partnerProductCode);
                continue;
            }
            if (cachedPartnerId != tvProgramme.partnerId) {
                cachedPartnerId = tvProgramme.partnerId;
                cachedByChannel = byPartnerId.get(cachedPartnerId);
                if (cachedByChannel == null) {
                    cachedByChannel = new HashMap<>();
                    byPartnerId.put(cachedPartnerId, cachedByChannel);
                }
                cachedTvChannel = null;
            }
            for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
                final String tvChannel = tvChannelCodes[i];
                if (tvChannel == null) {
                    log.error("replaceTvProgrammes(): the tv-programme has a null tv-channel at position " + i + ": ID=" + tvProgramme.id + ", partner_id=" + tvProgramme.partnerId + ", partner_product_code=" + tvProgramme.partnerProductCode);
                    continue;
                }
                if (!tvChannel.equals(cachedTvChannel)) {
                    cachedTvChannel = tvChannel;
                    cachedSet = cachedByChannel.get(tvChannel);
                    if (cachedSet == null) {
                        cachedSet = new TreeSet<>(TvProgrammeBeginTimeEquality.INSTANCE);
                        cachedByChannel.put(tvChannel, cachedSet);
                    }
                }
                cachedSet.add(tvProgramme);
            }
        }
        // perform the modifications
        byPartnerId.forEachEntry(new TLongObjectProcedure<HashMap<String, TreeSet<TvProgrammeProduct>>>() {
            @Override
            public boolean execute(final long partnerId, final HashMap<String, TreeSet<TvProgrammeProduct>> byTvChannels) {
                final PartnerProductCache cache = getPartnerProductCache(partnerId);
                byTvChannels.forEach((final String tvChannelCode, final TreeSet<TvProgrammeProduct> tvProgrammes) -> {

                });
                return true;
            }
        });
    }

    @Override
    public List<TvProgrammeInfo> tvProgrammesInIntervalForPartner(final Transaction transaction, final Partner partner, final long recommendTimeMillis, final long negativeOffsetMillis, final long positiveOffsetMillis) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final List<VideoData> data = new LinkedList<>();
        cache.findTvProgrammesBetweenTimestamps(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, null, data);
        final List<TvProgrammeInfo> result = new LinkedList<>();
        data.forEach((final VideoData videoData) -> {
            final List<TvChannelInfo> tvInfos = new LinkedList<>();
            videoData.availableTvChannels.forEach((final TvChannelData tvChannelProduct) -> tvInfos.add(new TvChannelInfo(tvChannelProduct.tvChannel, tvChannelProduct.productPackages)));
            result.add(new TvProgrammeInfo((TvProgrammeProduct)videoData.video, tvInfos));
        });
        log.debug("tvProgrammesInIntervalForPartner(): returning " + result.size() + " TV-programmes");
        return result;
    }

    @Override
    public List<VideoInfo> vodForPartner(final Transaction transaction, final Partner partner, final long recommendTimeMillis) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final Collection<Product> products = cache.findProductEntitiesOfProductType(dataLink.provider.getProductTypeCodes().idForVideo);
        if (products.size() == 0) return Collections.emptyList();
        final List<VideoInfo> result = new ArrayList<>(products.size());
        final Map<String, StringBuilder> missingVodsLog = new HashMap<>(1024);

        final Map<String, List<PackageProduct>> catalogueMapping = cache.currentCatalogueIdToPackagesMapping();
        for (final Product product : products) {
            if (product.deleted != null) continue; // skip deleted VODs
            final VideoProduct video;
            try {
                video = (VideoProduct) product;
            }
            catch (ClassCastException e) {
                log.error("The Entity is not of type VideoProduct, but " + (product == null ? "(null)" : product.getClass().getCanonicalName()) + ": " + e.toString());
                continue;
            }
            if ((video.validities != null) && (video.validities.length > 0)) {
                // check validity intervals - they are guaranteed sorted descending by end-time
                final int n = video.validities.length;
                int i = 0;
                while (i < n) {
                    final TimestampIntervalValue interval = video.validities[i];
                    if ((interval.beginMillis <= recommendTimeMillis) && (interval.endMillis > recommendTimeMillis)) break;
                    i++;
                }
                if (i >= n) continue; // no interval contains recommendTimeMillis
            }
            final String catalogueId = video.catalogueId;
            if (catalogueId == null) {
                continue;
            }
            final VodProduct vod = cache.findVodCatalogueByCatalogueId(catalogueId);
            if (vod == null) {
                StringBuilder sb = missingVodsLog.get(catalogueId);
                if (sb == null) {
                    sb = new StringBuilder(256);
                    sb.append("\n  ");
                    sb.append(catalogueId);
                    sb.append(": ");
                    sb.append(product.id);
                    missingVodsLog.put(catalogueId, sb);
                }
                else {
                    sb.append(", ");
                    sb.append(product.id);
                }
                continue;
            }
            final List<PackageProduct> productPackages = catalogueMapping.get(video.catalogueId);
            result.add(new VideoInfo(video, vod, productPackages == null ? Collections.<PackageProduct>emptyList() : productPackages));
        }
        if (missingVodsLog.size() > 0) {
            final StringBuilder sb = new StringBuilder();
            sb.append("vodForPartner(): There were SVOD or TVOD catalogues with the following IDs, referenced from the given products, but are missing (the referencing products were ignored):");
            for (final StringBuilder vodLog : missingVodsLog.values()) {
                sb.append(vodLog);
            }
            log.error(sb.toString());
        }
        log.debug("vodForPartner(): returning " + result.size() + " items out of " + products.size() + " candidates");
        return result;
    }

    @Override
    public TvProgrammeProduct tvProgrammeAtTimeForTvChannelAndPartner(final Transaction transaction, final Partner partner, final TvChannelProduct tvChannel, final long timeMillis) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final TvProgrammeProduct result = cache.findTvProgrammeAtTime(tvChannel.partnerProductCode, timeMillis);
        if (result == ProductCache.TV_PROGRAMME_TIME_OUT_OF_RANGE) {
            log.debug("[" + Thread.currentThread().getId() + "] tvProgrammeAtTimeForTvChannelAndPartner(): out of caching range for " + tvChannel.partnerProductCode + ": " + (timeMillis / 1000L));
            return getPhysicalManager().tvProgrammeAtTimeForTvChannelAndPartner(transaction, partner, tvChannel, timeMillis);
        }
        return result;
    }

    @Override
    public TvProgrammeProduct firstTvProgrammeAfterTimeForTvChannelAndPartner(final Transaction transaction, final Partner partner, final TvChannelProduct tvChannel, final long timeMillis) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final TvProgrammeProduct result = cache.findFirstTvProgrammeAfterTime(tvChannel.partnerProductCode, timeMillis);
        if (result == ProductCache.TV_PROGRAMME_TIME_OUT_OF_RANGE) return getPhysicalManager().firstTvProgrammeAfterTimeForTvChannelAndPartner(transaction, partner, tvChannel, timeMillis);
        return result;
    }

    @Override
    public VodProduct getVodCatalogue(final Transaction transaction, final Partner partner, final String catalogueId) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        return cache.findVodCatalogueByCatalogueId(catalogueId);
    }

    @Override
    public List<PackageProduct> getPackagesForCatalogue(final Transaction transaction, final Partner partner, final String catalogueId) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        return cache.currentCatalogueIdToPackagesMapping().get(catalogueId);
    }

    @Override
    public List<PackageProduct> getPackagesForTvChannel(final Transaction transaction, final Partner partner, final String tvChannelCode) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        final TvChannelData data = cache.currentTvChannelCodeToPackageMapping().get(tvChannelCode);
        return data == null ? null : data.productPackages;
    }

    @Override
    public Set<Matcher> getMatchers(final Transaction transaction, final MatcherKey key) {
        return dataLink.provider.getProductMatcherCache().getMatchers(transaction, key);
    }

    /**
     * Use this method asynchronously and atomically, not e.g. within processing of a product, or else you may get a deadlock!
     * This method commits all changes in caches as they happen!
     *
     * @param transaction the transaction to use
     * @param attribute the attribute to modify
     * @param oldValue the old value to be modified
     * @param newValue the new value to be set
     */
    @Deprecated
    @Override
    public void setRelatedFieldForAll(final Transaction transaction, final Attribute attribute, final Value oldValue, final Value newValue) {
        getPhysicalManager().setRelatedFieldForAll(transaction, attribute, oldValue, newValue);
        dataLink.provider.getProductMatcherCache().setRelatedFieldForAll(transaction, attribute, oldValue, newValue);
        dataLink.provider.withEachPartnerProductCache(new PartnerProductCacheProcedure() {
            @Override
            public boolean execute(final PartnerProductCache cache) {
                cache.setRelatedFieldForAll(transaction, attribute, oldValue, newValue);
                return true;
            }
        });
    }

    private static final class PackageListWithSubscriptionStatus {
        final List<PackageProduct> productPackages;
        final boolean isSubscribed;
        final VodProduct vod;
        PackageListWithSubscriptionStatus(final List<PackageProduct> productPackages, final boolean isSubscribed, final VodProduct vod) {
            this.productPackages = productPackages;
            this.isSubscribed = isSubscribed;
            this.vod = vod;
        }
    }

    @Override
    public void tvProgrammesInIntervalForPartner(
            final Transaction transaction,
            final Partner partner,
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        getPartnerProductCache(partner.getId()).reachableTvProgrammesInRange(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, subscriptionPackageIds, outputBuilder);
    }

    @Override
    public void vodForPartner(
            final Transaction transaction,
            final Partner partner,
            final long recommendTimeMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        if (cache == null) {
            log.error("vodForPartner(): no partner product cache for partner " + partner.getId());
            return;
        }
        final Collection<Product> products = cache.findProductEntitiesOfProductType(dataLink.provider.getProductTypeCodes().idForVideo);
        if (products.isEmpty()) {
            log.warn("vodForPartner(): no video products for partner " + partner.getId());
            return;
        }
        final StringBuilder logBuilder = new StringBuilder(1024);
        final Set<String> missingVods = new HashSet<>();

        final Map<String, List<PackageProduct>> catalogueMappingRaw = cache.currentCatalogueIdToPackagesMapping();
        // compute subscription status of each catalogue
        final Map<String, PackageListWithSubscriptionStatus> catalogueMapping = new HashMap<>(catalogueMappingRaw.size());
        catalogueMappingRaw.forEach((final String catalogueId, final List<PackageProduct> packages) -> {
            final VodProduct vod = cache.findVodCatalogueByCatalogueId(catalogueId);
            if (vod == null) {
                missingVods.add(catalogueId);
                return;
            }
            boolean isSubscribed = false;
            for (final PackageProduct p : packages) {
                if (subscriptionPackageIds.contains(p.id)) {
                    isSubscribed = true;
                    break;
                }
            }
            catalogueMapping.put(catalogueId, new PackageListWithSubscriptionStatus(packages, isSubscribed, vod));
        });
        if (!missingVods.isEmpty()) {
            // log missing VODs
            final Iterator<String> it = missingVods.iterator();
            logBuilder.append("\nFound packages containing the following catalogue-ids, but no VOD product exists for them:");
            while (it.hasNext()) {
                final String catalogueId = it.next();
                logBuilder.append("\n    ").append(catalogueId).append(" referenced from: ");
                final List<PackageProduct> packages = catalogueMappingRaw.get(catalogueId);
                if ((packages == null) || packages.isEmpty()) logBuilder.append("(none)");
                else {
                    final Iterator<PackageProduct> pit = packages.iterator();
                    logBuilder.append(pit.next());
                    while (pit.hasNext()) logBuilder.append(", ").append(pit.next());
                }
            }
        }

        // build VideoData for each product, and pass it to the DataSet builder
        final Map<String, StringBuilder> missingVodsLog = new HashMap<>(1024);
        final int startingSize = outputBuilder.size();
        final int[] countProcessed = new int[1];
        products.forEach((final Product product) -> {
            if (product.deleted != null) return/*continue*/; // skip deleted VODs
            final VideoProduct video;
            try {
                video = (VideoProduct) product;
            }
            catch (ClassCastException e) {
                log.error("The Entity is not of type VideoProduct, but " + (product == null ? "(null)" : product.getClass().getCanonicalName()) + ": " + e.toString());
                return/*continue*/;
            }
            if ((video.validities != null) && (video.validities.length > 0)) {
                // check validity intervals - they are guaranteed sorted descending by end-time
                final int n = video.validities.length;
                int i = 0;
                while (i < n) {
                    final TimestampIntervalValue interval = video.validities[i];
                    if ((interval.beginMillis <= recommendTimeMillis) && (interval.endMillis > recommendTimeMillis)) break;
                    i++;
                }
                if (i >= n) return/*continue*/; // no interval contains recommendTimeMillis
            }
            final String catalogueId = video.catalogueId;
            if (catalogueId == null) {
                return/*continue*/;
            }
            final PackageListWithSubscriptionStatus catalogueInfo = catalogueMapping.get(catalogueId);
            if (catalogueInfo == null) {
                final StringBuilder sb = missingVodsLog.get(catalogueId);
                if (sb == null) {
                    missingVodsLog.put(catalogueId, new StringBuilder(256).append("\n    ").append(catalogueId).append(" referenced from: ").append(product.id));
                }
                else {
                    sb.append(", ").append(product.id);
                }
                return/*continue*/;
            }
            outputBuilder.add(VideoData.forVideo(video, catalogueInfo.vod, catalogueInfo.productPackages, catalogueInfo.isSubscribed));
            countProcessed[0]++;
        });
        if (!missingVodsLog.isEmpty()) {
            logBuilder.append("\nThere were VOD catalogues with the following IDs, referenced from the given videos, but the SVOD/TVOD products representing the VOD catalogues are missing or no subscription package contains them (the referencing videos were ignored):");
            missingVodsLog.forEach((final String catalogueId, final StringBuilder vodLog) -> {
                logBuilder.append(vodLog);
            });
        }

        // make a log entry
        final StringBuilder sb = new StringBuilder(128 + logBuilder.length());
        sb.append("vodForPartner(): processed ").append(countProcessed[0]).append(" products out of ").append(products.size()).append(" candidates, passed ").append(outputBuilder.size() - startingSize).append(" unique products to the builder");
        if (logBuilder.length() > 0) sb.append(logBuilder);
        log.debug(sb.toString());
    }

    private static final class LazyVideoInfoEvaluator {
        final PartnerProductCache cache;
        final Map<String, List<PackageProduct>> catalogueMappingRaw;
        final TLongSet subscriptionPackageIds;
        final Map<String, PackageListWithSubscriptionStatus> catalogueMapping = new HashMap<>();

        LazyVideoInfoEvaluator(final PartnerProductCache cache, final TLongSet subscriptionPackageIds) {
            this.cache = cache;
            this.catalogueMappingRaw = cache.currentCatalogueIdToPackagesMapping();
            this.subscriptionPackageIds = subscriptionPackageIds;
        }

        PackageListWithSubscriptionStatus getCatalogueInfo(final String catalogueId) {
            final PackageListWithSubscriptionStatus existing = catalogueMapping.get(catalogueId);
            if (existing != null) return existing;
            final List<PackageProduct> packages = catalogueMappingRaw.get(catalogueId);
            if ((packages == null) || packages.isEmpty()) {
                return null;
            }
            final VodProduct vod = cache.findVodCatalogueByCatalogueId(catalogueId);
            if (vod == null) {
                return null;
            }
            boolean isSubscribed = false;
            for (final PackageProduct p : packages) {
                if (subscriptionPackageIds.contains(p.id)) {
                    isSubscribed = true;
                    break;
                }
            }
            final PackageListWithSubscriptionStatus result = new PackageListWithSubscriptionStatus(packages, isSubscribed, vod);
            catalogueMapping.put(catalogueId, result);
            return result;
        }
    }

    private static final class TvChannelListWithSubscriptionStatus {
        final List<TvChannelData> availableTvChannels;
        final boolean isSubscribed;
        TvChannelListWithSubscriptionStatus(final List<TvChannelData> availableTvChannels, final boolean isSubscribed) {
            this.availableTvChannels = availableTvChannels;
            this.isSubscribed = isSubscribed;
        }
    }

    private static final class LazyTvProgrammeInfoEvaluator {
        final PartnerProductCache cache;
        final TLongSet subscriptionPackageIds;
        final Map<String, TvChannelData> tvChannelPackages;
        final Map<String, TvChannelData> dataMapping = new HashMap<>();
        final Map<String, TvChannelListWithSubscriptionStatus> tvChannelMapping = new HashMap<>();

        LazyTvProgrammeInfoEvaluator(final PartnerProductCache cache, final TLongSet subscriptionPackageIds) {
            this.cache = cache;
            this.subscriptionPackageIds = subscriptionPackageIds;
            this.tvChannelPackages = cache.currentTvChannelCodeToPackageMapping();
        }

        TvChannelListWithSubscriptionStatus getTvChannelsInfo(final String[] tvChannelCodes) {
            final String concatenatedCodes = concat(tvChannelCodes);
            final TvChannelListWithSubscriptionStatus status = tvChannelMapping.get(concatenatedCodes);
            if (status != null) return status;
            final List<TvChannelData> tvChannelDatas = new ArrayList<>();
            boolean isSubscribed = false;
            for (final String tvChannelCode : tvChannelCodes) {
                final TvChannelData existingData = dataMapping.get(tvChannelCode);
                if (existingData == null) {
                    final TvChannelData tvChannelData = tvChannelPackages.get(tvChannelCode);
                    if (tvChannelData == null) {
                        log.error("There are no subscription packages for TV-channel with partner_product_code=" + tvChannelCode);
                        continue;
                    }
                    boolean tvChannelIsSubscribed = false;
                    for (final PackageProduct p : tvChannelData.productPackages) {
                        if (subscriptionPackageIds.contains(p.id)) {
                            isSubscribed = true;
                            tvChannelIsSubscribed = true;
                            break;
                        }
                    }
                    final TvChannelData newData = new TvChannelData(tvChannelData.tvChannel, tvChannelData.productPackages, tvChannelIsSubscribed);
                    dataMapping.put(tvChannelCode, newData);
                    tvChannelDatas.add(newData);
                }
                else {
                    tvChannelDatas.add(existingData);
                    if (existingData.isSubscribed) isSubscribed = true;
                }
            }
            final TvChannelListWithSubscriptionStatus newStatus = new TvChannelListWithSubscriptionStatus(tvChannelDatas, isSubscribed);
            tvChannelMapping.put(concatenatedCodes, newStatus);
            return newStatus;
        }

        private final StringBuilder concatenator = new StringBuilder(50);

        String concat(final String[] codes) {
            if ((codes == null) || (codes.length == 0)) return "";
            if (codes.length == 1) return codes[0];
            final StringBuilder c = concatenator; // local variable, for faster access
            c.setLength(0);
            c.append(codes[0]);
            final int n = codes.length;
            for (int i = 1; i < n; i++) c.append(" ").append(codes[i]);
            return c.toString();
        }
    }

    @Override
    public void buildDatasetFromVideos(
            final Transaction transaction,
            final Iterable<? extends GeneralVideoProduct> videos,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        final TLongObjectMap<LazyVideoInfoEvaluator> partnerVideoEvaluators = new TLongObjectHashMap<>();
        final TLongObjectMap<LazyTvProgrammeInfoEvaluator> partnerTvProgrammeEvaluators = new TLongObjectHashMap<>();

        for (final GeneralVideoProduct p : videos) {
            if (p instanceof VideoProduct) {
                final VideoProduct video = (VideoProduct)p;
                final String catalogueId = video.catalogueId;
                if (catalogueId == null) {
                    log.error("buildDatasetFromVideos(): Video product with product_id=" + video.id + " has no catalogue ID");
                    continue;
                }
                LazyVideoInfoEvaluator evaluator = partnerVideoEvaluators.get(video.partnerId);
                if (evaluator == null) {
                    final PartnerProductCache cache = getPartnerProductCache(video.partnerId);
                    if (cache == null) {
                        log.error("buildDatasetFromVideos(): no partner product cache for partner " + video.partnerId);
                        continue;
                    }
                    evaluator = new LazyVideoInfoEvaluator(cache, subscriptionPackageIds);
                    partnerVideoEvaluators.put(video.partnerId, evaluator);
                }
                final PackageListWithSubscriptionStatus catalogueInfo = evaluator.getCatalogueInfo(catalogueId);
                if (catalogueInfo == null) {
                    log.error("buildDatasetFromVideos(): Could not determine catalogue info for video product with product_id=" + video.id + " having catalogue ID " + catalogueId);
                    continue;
                }
                outputBuilder.add(VideoData.forVideo(video, catalogueInfo.vod, catalogueInfo.productPackages, catalogueInfo.isSubscribed));
            }
            else if (p instanceof TvProgrammeProduct) {
                final TvProgrammeProduct tvProgramme = (TvProgrammeProduct)p;
                final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
                if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                    log.error("buildDatasetFromVideos(): TV-programme product with product_id=" + tvProgramme.id + " has no TV-channel codes assigned");
                    continue;
                }
                LazyTvProgrammeInfoEvaluator evaluator = partnerTvProgrammeEvaluators.get(tvProgramme.partnerId);
                if (evaluator == null) {
                    final PartnerProductCache cache = getPartnerProductCache(tvProgramme.partnerId);
                    if (cache == null) {
                        log.error("buildDatasetFromVideos(): no partner product cache for partner " + tvProgramme.partnerId);
                        continue;
                    }
                    evaluator = new LazyTvProgrammeInfoEvaluator(cache, subscriptionPackageIds);
                    partnerTvProgrammeEvaluators.put(tvProgramme.partnerId, evaluator);
                }
                final TvChannelListWithSubscriptionStatus status = evaluator.getTvChannelsInfo(tvChannelCodes);
                if (status == null) {
                    log.error("buildDatasetFromVideos(): Could not determine TV-channels info for tv-programme product with product_id=" + tvProgramme.id);
                    continue;
                }
                outputBuilder.add(VideoData.forTvProgramme(tvProgramme, status.availableTvChannels, status.isSubscribed));
            }
            else {
                throw new DatabaseException("Unknown video product in the list of supplied videos: " + p.getClass().getCanonicalName() + ", product_id=" + p.id);
            }
        }
    }

    @Override
    public List<TvChannelProduct> getTvChannelsForPartner(final Transaction transaction, final Partner partner) {
        final PartnerProductCache cache = getPartnerProductCache(partner.getId());
        if (cache == null) {
            log.error("getTvChannelsForPartner(): no partner product cache for partner " + partner.getId());
            return Collections.emptyList();
        }
        final Collection<Product> tvChannels = cache.findProductEntitiesOfProductType(dataLink.getProvider().getProductTypeCodes().idForTvChannel);
        final List<TvChannelProduct> result = new ArrayList<>(tvChannels.size());
        for (final Product p : tvChannels) {
            try {
                result.add((TvChannelProduct)p);
            }
            catch (ClassCastException e) {
                log.error("Failed to cast a Product of class " + p.getClass().getCanonicalName() + " to the TvChannelProduct: " + e.toString(), e);
            }
        }
        return result;
    }


    static final class ProductCacheSeeker implements PartnerProductCacheProcedure {
        private final long soughtProductId;
        public Product foundProduct = null;

        public ProductCacheSeeker(final long productId) {
            this.soughtProductId = productId;
        }

        @Override
        public boolean execute(final PartnerProductCache cache) {
            final Product p = cache.getProduct(soughtProductId);
            if (p != null) {
                foundProduct = p;
                return false;
            }
            return true;
        }
    }

    static final class ProductCacheLockedSeeker implements PartnerProductCacheProcedure {
        private final Transaction transaction;
        private final long soughtProductId;
        public Product foundProduct = null;

        public ProductCacheLockedSeeker(final Transaction transaction, final long productId) {
            this.transaction = transaction;
            this.soughtProductId = productId;
        }

        @Override
        public boolean execute(final PartnerProductCache cache) {
            final Product p = cache.getProductLocked(transaction, soughtProductId);
            if (p != null) {
                foundProduct = p;
                return false;
            }
            return true;
        }
    }
}
