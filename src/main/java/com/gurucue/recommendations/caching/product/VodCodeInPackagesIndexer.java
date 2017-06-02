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
 * Reverse indexer: given a SVOD/TVOD code return a list of packages containing subscriptions to svod/tvod.
 * Indexes only package products having svod-id attributes.
 */
public final class VodCodeInPackagesIndexer extends MultikeyIndexDefinition<String, Product> {
    private static final Logger log = LogManager.getLogger(VodCodeInPackagesIndexer.class);
    private final long packageProductTypeId;

    public VodCodeInPackagesIndexer(final long packageProductTypeId) {
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
        final String[] vodCodes;
        try {
            vodCodes = ((PackageProduct)product).vodCodes;
        }
        catch (ClassCastException e) {
            log.error("Trying to index a product of type packages, but it is not a PackageProduct instance: " + product.getClass().getCanonicalName(), e);
            return Collections.emptyList();
        }
        if ((vodCodes == null) || (vodCodes.length == 0)) return Collections.emptyList(); // no svod-id attributes in this package
        final List<String> result = new ArrayList<>(vodCodes.length);
        for (final String value : vodCodes) result.add(value);
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
