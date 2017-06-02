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
import com.gurucue.recommendations.caching.product.PartnerProductCacheProcedure;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.CloseListener;
import com.gurucue.recommendations.data.ConsumerEventTypeCodes;
import com.gurucue.recommendations.data.ConsumerListener;
import com.gurucue.recommendations.data.DataLink;
import com.gurucue.recommendations.data.DataTypeCodes;
import com.gurucue.recommendations.data.LanguageCodes;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;
import com.gurucue.recommendations.entity.ConsumerEvent;
import com.gurucue.recommendations.entity.Partner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * A caching data provider, acting as a proxy layer to another data provider.
 */
public final class CachedJdbcDataProvider implements JdbcDataProvider {
    private static final Logger log = LogManager.getLogger(CachedJdbcDataProvider.class);
    private static final int CACHE_SLOTS = 100;

    public static CachedJdbcDataProvider create(final JdbcDataProvider parent) {
        final long startNano = System.nanoTime();
        try {
            return new CachedJdbcDataProvider(parent);
        }
        finally {
            final long endNano = System.nanoTime();
            log.info("Provider initialized in " + (endNano - startNano) + " ns");
        }
    }

    final JdbcDataProvider parent;

    // caching layer

    private final PartnerProductCache[] partnerProductCaches = new PartnerProductCache[CACHE_SLOTS]; // for simplicity and speed: we don't deal with mappings here
    final ConsumerCache consumerCache;
    final InternalProductCache productMatcherCache;

    private CachedJdbcDataProvider(final JdbcDataProvider parent) {
        final long nsStart = System.nanoTime();
        this.parent = parent;

        // parallel cache initialization: pre-cache everything we know of -- for now only consumers and products

        final FutureTask<ConsumerCache> consumerCacheTask = new FutureTask<>(new Callable<ConsumerCache>() {
            @Override
            public ConsumerCache call() throws Exception {
                return new ConsumerCache(parent);
            }
        });

        final FutureTask<InternalProductCache> internalProductCacheTask = new InternalProductCacheCreator(parent);

        final List<Partner> partners;
        try (final JdbcDataLink link = parent.newJdbcDataLink()) {
            partners = link.getPartnerManager().list();
        }

        final List<PartnerProductCacheCreator> productCacheCreators = new ArrayList<>(partners.size());

        final ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            // submit jobs

            executor.submit(consumerCacheTask);
            for (final Partner partner : partners) {
                final long partnerId = partner.getId();
                if (partnerId < 1L) { // only create cache for real partners
                    log.info("Not caching products for partner " + partner.getUsername() + " (" + partnerId + "): only positive partner IDs are eligible for product caching");
                    continue;
                }
                if (partnerId >= CACHE_SLOTS) {
                    log.error("Not caching products for partner " + partner.getUsername() + " (" + partnerId + "): ID out of range, only " + CACHE_SLOTS + " product cache slots available");
                    continue;
                }
                final PartnerProductCacheCreator ppcc = new PartnerProductCacheCreator(partner, parent);
                productCacheCreators.add(ppcc);
                executor.submit(ppcc);
            }
            executor.submit(internalProductCacheTask);

            // reap jobs

            for (final PartnerProductCacheCreator ppcc : productCacheCreators) {
                try {
                    final PartnerProductCache cache = ppcc.get();
                    partnerProductCaches[ppcc.partner.getId().intValue()] = cache;
                } catch (CancellationException e) {
                    throw new DatabaseException("Cancelled while creating product cache for partner " + ppcc.partner.getUsername() + ": " + e.toString(), e);
                } catch (InterruptedException e) {
                    throw new DatabaseException("Interrupted while creating product cache for partner " + ppcc.partner.getUsername() + ": " + e.toString(), e);
                } catch (ExecutionException e) {
                    throw new DatabaseException("Execution exception while creating product cache for partner " + ppcc.partner.getUsername() + ": " + e.toString(), e);
                }
            }

            try {
                consumerCache = consumerCacheTask.get();
            } catch (CancellationException e) {
                throw new DatabaseException("Cancelled while creating consumer cache: " + e.toString(), e);
            } catch (InterruptedException e) {
                throw new DatabaseException("Interrupted while creating consumer cache: " + e.toString(), e);
            } catch (ExecutionException e) {
                throw new DatabaseException("Execution exception while creating consumer cache: " + e.toString(), e);
            }

            try {
                productMatcherCache = internalProductCacheTask.get();
            } catch (CancellationException e) {
                throw new DatabaseException("Cancelled while creating consumer cache: " + e.toString(), e);
            } catch (InterruptedException e) {
                throw new DatabaseException("Interrupted while creating consumer cache: " + e.toString(), e);
            } catch (ExecutionException e) {
                throw new DatabaseException("Execution exception while creating consumer cache: " + e.toString(), e);
            }
        }
        finally {
            executor.shutdownNow();
        }

        log.info("Caching data provider initialized in " + (System.nanoTime() - nsStart) + " ns");
    }

    @Override
    public JdbcDataLink newJdbcDataLink() {
        return new CachedJdbcDataLink(this);
    }

    @Override
    public DataLink newDataLink() {
        return newJdbcDataLink();
    }

    @Override
    public void clearCaches() {
        parent.clearCaches();
        // TODO: purge caching layer?
    }

    @Override
    public void close() {
        for (int i = partnerProductCaches.length - 1; i >= 0; i--) {
            final PartnerProductCache cache = partnerProductCaches[i];
            if (cache != null) cache.destroy();
        }
        consumerCache.destroy();
        parent.close();
    }

    @Override
    public void registerOnClose(final CloseListener closeListener) {
        parent.registerOnClose(closeListener);
    }

    @Override
    public void runAsync(final Runnable job) {
        parent.runAsync(job);
    }

    @Override
    public AttributeCodes getAttributeCodes() {
        return parent.getAttributeCodes();
    }

    @Override
    public ProductTypeCodes getProductTypeCodes() {
        return parent.getProductTypeCodes();
    }

    @Override
    public ConsumerEventTypeCodes getConsumerEventTypeCodes() {
        return parent.getConsumerEventTypeCodes();
    }

    @Override
    public DataTypeCodes getDataTypeCodes() {
        return parent.getDataTypeCodes();
    }

    @Override
    public LanguageCodes getLanguageCodes() {
        return parent.getLanguageCodes();
    }

    @Override
    public void queueConsumerEvent(final ConsumerEvent consumerEvent) throws InterruptedException {
        parent.queueConsumerEvent( consumerEvent);
    }

    @Override
    public void resizeConsumerEventQueueSize(final int newSize) {
        parent.resizeConsumerEventQueueSize(newSize);
    }

    @Override
    public void resizeConsumerEventQueueThreadPool(final int newSize) {
        parent.resizeConsumerEventQueueThreadPool(newSize);
    }

    @Override
    public void registerConsumerListener(final ConsumerListener listener) {
        // TODO: is this correct?
        parent.registerConsumerListener(listener);
    }

    @Override
    public void unregisterConsumerListener(final ConsumerListener listener) {
        // TODO: is this correct?
        parent.unregisterConsumerListener(listener);
    }

    JdbcDataLink newPhysicalLink() {
        return parent.newJdbcDataLink();
    }

    PartnerProductCache getProductCacheForPartner(final long partnerId) {
        final int index = (int)partnerId;
        if ((index < 1) || (index >= CACHE_SLOTS)) {
            log.warn("Requested product cache is out of cacheable partner range, returning null: partnerId=" + partnerId);
            return null;
        }
        final PartnerProductCache cache = partnerProductCaches[index];
        if (cache != null) return cache;
        synchronized (partnerProductCaches) {
            final PartnerProductCache oldCache = partnerProductCaches[index];
            if (oldCache != null) return oldCache;
            log.info("Product cache for partner " + partnerId + " not found in cache, instantiating and caching it now");
            final PartnerProductCache newCache = new PartnerProductCache(partnerId, parent);
            partnerProductCaches[index] = newCache;
            return newCache;
        }
    }

    InternalProductCache getProductMatcherCache() {
        return productMatcherCache;
    }

    void withEachPartnerProductCache(final PartnerProductCacheProcedure procedure) {
        for (int i = partnerProductCaches.length - 1; i >= 0; i--) {
            final PartnerProductCache cache = partnerProductCaches[i];
            if (cache != null) {
                if (!procedure.execute(cache)) break;
            }
        }
    }

    private static final class PartnerProductCacheCreator extends FutureTask<PartnerProductCache> {
        public final Partner partner;

        public PartnerProductCacheCreator(final Partner partner, final JdbcDataProvider provider) {
            super(new Callable<PartnerProductCache>() {
                @Override
                public PartnerProductCache call() throws Exception {
                    return new PartnerProductCache(partner.getId(), provider);
                }
            });
            this.partner = partner;
        }
    }

    private static final class InternalProductCacheCreator extends FutureTask<InternalProductCache> {
        public InternalProductCacheCreator(final JdbcDataProvider provider) {
            super(new Callable<InternalProductCache>() {
                @Override
                public InternalProductCache call() throws Exception {
                    return new InternalProductCache(Partner.PARTNER_ZERO_ID, provider);
                }
            });
        }
    }
}
