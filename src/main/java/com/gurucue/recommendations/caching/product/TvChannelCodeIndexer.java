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
import com.gurucue.recommendations.caching.UnikeyIndexDefinition;
import com.gurucue.recommendations.entity.product.Product;

/**
 * Index tv-channel products of a partner by their partner code.
 */
public final class TvChannelCodeIndexer extends UnikeyIndexDefinition<String, Product> {
    private final long tvChannelProductTypeId;

    public TvChannelCodeIndexer(final long tvChannelProductTypeId) {
        this.tvChannelProductTypeId = tvChannelProductTypeId;
    }

    @Override
    public void changed(final Index<String, Product> index, final Product before, final Product after) {
        final String beforeKey = before == null ? null : key(before);
        final String afterKey = after == null ? null : key(after);
        if (beforeKey == null) {
            if ((afterKey == null) || (after.deleted != null)) return; // don't index deleted products
            index.internalAdd(afterKey, after);
        }
        else if ((afterKey == null) || (after.deleted == null)) { // don't index deleted products
            index.internalRemove(beforeKey, before);
        }
        else {
            if (beforeKey.equals(afterKey)) index.internalReplace(beforeKey, before, after);
            else {
                index.internalAdd(afterKey, after);
                index.internalRemove(beforeKey, before);
            }
        }
    }

    @Override
    public String key(final Product product) {
        if (product == null) return null;
        if (product.productTypeId == tvChannelProductTypeId) {
            // index only tv-channel entities
            return product.partnerProductCode;
        }
        return null;
    }
}
