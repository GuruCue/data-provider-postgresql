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

import com.gurucue.recommendations.ResponseException;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.product.Product;

import java.sql.Timestamp;

/**
 * Interface used for instantiating a certain Product sub-class at cache fill,
 * which usually occurs only at the application startup.
 */
public interface ProductCreator {
    // productTypeId and partnerId are supposed to be fixed
    // aswell: provider, log
    ProductType getProductType();
    Product create(long id, String partnerProductCode, String jsonAttributes, String jsonRelated, Timestamp added, Timestamp modified, Timestamp deleted) throws ResponseException;
}
