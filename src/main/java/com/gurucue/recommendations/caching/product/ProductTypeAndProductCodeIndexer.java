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

import com.gurucue.recommendations.caching.UnikeyIndexDefinition;
import com.gurucue.recommendations.entity.product.Product;

/**
 * Index products of a partner according to their product type and partner's product code.
 */
public final class ProductTypeAndProductCodeIndexer extends UnikeyIndexDefinition<KeyProductTypeAndProductCode, Product> {
    @Override
    public KeyProductTypeAndProductCode key(final Product product) {
        if (product == null) return null;
        return new KeyProductTypeAndProductCode(product.productTypeId, product.partnerProductCode);
    }
}
