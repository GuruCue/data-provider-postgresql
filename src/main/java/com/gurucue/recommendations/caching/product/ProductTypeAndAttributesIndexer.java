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

import com.gurucue.recommendations.caching.MultikeyIndexDefinition;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.entity.product.Product;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;

/**
 * A variant index: indexes by product type and any given attributes.
 */
public final class ProductTypeAndAttributesIndexer extends MultikeyIndexDefinition<KeyProductTypeAndAttributes, Product> {
    private static final Logger log = LogManager.getLogger(ProductTypeAndAttributesIndexer.class);
    private final AttributeIds attributeIds;
    private final AttributeCodes attributeCodes;

    public ProductTypeAndAttributesIndexer(final AttributeIds attributeIds, final AttributeCodes attributeCodes) {
        this.attributeIds = attributeIds;
        this.attributeCodes = attributeCodes;
    }

    @Override
    public Collection<KeyProductTypeAndAttributes> keys(final Product product) {
        if (product == null) {
            // ignore
            log.error("Null entity");
            return Collections.emptyList();
        }
        return KeyProductTypeAndAttributes.createKeys(product.productTypeId, attributeIds, product.attributes.values, attributeCodes);
    }
}
