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
package com.gurucue.recommendations.caching.product;

import com.gurucue.recommendations.caching.Index;
import com.gurucue.recommendations.caching.MultikeyIndexDefinition;
import com.gurucue.recommendations.entity.product.PackageProduct;
import com.gurucue.recommendations.entity.product.Product;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reverse indexer: given a tv-channel code return a list of packages containing subscription to the tv-channel.
 * Indexes only package products having tv-channel-id attributes.
 */
public final class TvChannelInPackagesIndexer extends MultikeyIndexDefinition<String, Product> {
    private static final Logger log = LogManager.getLogger(TvChannelInPackagesIndexer.class);
    private final long packageProductTypeId;

    public TvChannelInPackagesIndexer(final long packageProductTypeId) {
        this.packageProductTypeId = packageProductTypeId;
    }

    @Override
    public Collection<String> keys(final Product product) {
        if (product == null) {
            // ignore
            log.error("Null entity");
            return Collections.emptyList();
        }
        if (product.productTypeId != packageProductTypeId) return Collections.emptyList(); // only index package products
        final String[] tvChannelCodes;
        try {
            tvChannelCodes = ((PackageProduct)product).tvChannelCodes;
        }
        catch (ClassCastException e) {
            log.error("Trying to index a product of type packages, but it is not a PackageProduct instance: " + product.getClass().getCanonicalName(), e);
            return Collections.emptyList();
        }
        if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) return Collections.emptyList(); // no tv-channel-id attributes in this package
        final List<String> result = new ArrayList<>(tvChannelCodes.length);
        for (final String value : tvChannelCodes) result.add(value);
        return result;
    }

    @Override
    public void changed(final Index<String, Product> index, final Product before, final Product after) {
        if (before == null) {
            if ((after == null) || (after.deleted != null)) return; // don't index deleted products
            for (final String key : keys(after)) {
                index.internalAdd(key, after);
            }
        }
        else if ((after == null) || (after.deleted != null)) { // don't index deleted products
            for (final String key : keys(before)) {
                index.internalRemove(key, before);
            }
        }
        else {
            final Set<String> beforeKeys = new HashSet<>(keys(before));
            for (final String key : keys(after)) {
                if (beforeKeys.remove(key)) index.internalReplace(key, before, after);
                else index.internalAdd(key, after);
            }
            for (final String key : beforeKeys) {
                index.internalRemove(key, before);
            }
        }
    }
}
