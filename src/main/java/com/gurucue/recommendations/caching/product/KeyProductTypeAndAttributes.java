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

import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Key a product by its product type and an arbitrary list of attribute values.
 * Only one value per attribute.
 */
public final class KeyProductTypeAndAttributes {
    private final long productTypeId;
    private final AttributeIds attributeIds;
    private final String[] values;
    private final int hash;

    public KeyProductTypeAndAttributes(final long productTypeId, final AttributeIds attributeIds, final String[] values) {
        this.productTypeId = productTypeId;
        this.attributeIds = attributeIds;
        this.values = values;
        int hash = 17;
        hash = (31 * hash) + (int)(productTypeId ^ (productTypeId >>> 32));
        hash = (31 * hash) + attributeIds.hashCode();
        for (int i = 0; i < values.length; i++) {
            hash = (31 * hash) + values[i].hashCode();
        }
        this.hash = hash;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof KeyProductTypeAndAttributes)) return false;
        final KeyProductTypeAndAttributes other = (KeyProductTypeAndAttributes)obj;
        if (!other.attributeIds.equals(this.attributeIds)) return false;
        if (other.productTypeId != this.productTypeId) return false;
        if (other.values.length != this.values.length) return false;
        for (int i = this.values.length - 1; i >= 0; i--) {
            if (!other.values[i].equals(this.values[i])) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static List<KeyProductTypeAndAttributes> createKeys(final long productTypeId, final AttributeIds attributeIds, final Map<Attribute, Value> attributeValues, final AttributeCodes attributeCodes) {
        // collect a list of attribute value lists
        final long[] ids = attributeIds.ids;
        final int n = ids.length;
        final String[][] grandValuesList = new String[n][];
        final int[] listCounters = new int[n];
        final int[] listSizes = new int[n];
        for (int i = n - 1; i >= 0; i--) {
            final Value value = attributeValues.get(attributeCodes.byId(ids[i]));
            if (value == null) return Collections.emptyList(); // not indexable: an attribute values is missing
            final String[] valueList = value.asStrings();
            if ((valueList == null) || (valueList.length == 0)) return Collections.emptyList(); // not indexable: an attribute values is missing
            grandValuesList[i] = valueList;
            listCounters[i] = listSizes[i] = valueList.length - 1;
        }
        // multiply values: enumerate all permutations, holding attribute locations fixed
        final List<KeyProductTypeAndAttributes> result = new ArrayList<>();
        int c = 1;
        while (c >= 0) {
            final String[] values = new String[n]; // one value per attribute
            for (int i = 0; i < listCounters.length; i++) {
                final int idx = listCounters[i];
                values[i] = grandValuesList[i][idx];
            }
            result.add(new KeyProductTypeAndAttributes(productTypeId, attributeIds, values));
            // prepare indexes for the next permutation
            int i = listCounters.length - 1;
            c = listCounters[i] = listCounters[i] - 1;
            while (i > 0) {
                if (c < 0) {
                    listCounters[i] = listSizes[i];
                    i--;
                    c = listCounters[i] = listCounters[i] - 1;
                }
                else break;
            }
        }
        return result;
    }
}
