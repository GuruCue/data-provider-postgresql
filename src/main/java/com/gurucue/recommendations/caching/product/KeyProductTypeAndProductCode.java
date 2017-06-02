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

public final class KeyProductTypeAndProductCode {
    final long productTypeId;
    final String productCode;
    final int hash;

    public KeyProductTypeAndProductCode(final long productTypeId, final String productCode) {
        this.productTypeId = productTypeId;
        this.productCode = productCode;
        // recipe taken from Effective Java, 2nd edition (ISBN 978-0-321-35668-0), page 47
        int hash = 17;
        hash = (31 * hash) + (int)(productTypeId ^ (productTypeId >>> 32));
        if (productCode != null) hash = (31 * hash) + productCode.hashCode();
        this.hash = hash;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof KeyProductTypeAndProductCode)) return false;
        final KeyProductTypeAndProductCode other = (KeyProductTypeAndProductCode)obj;
        if (other.productTypeId != productTypeId) return false;
        if (productCode == null) return other.productCode == null;
        return productCode.equals(other.productCode);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
