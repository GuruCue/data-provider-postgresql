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

import com.gurucue.recommendations.entity.Attribute;

import java.util.Arrays;
import java.util.Collection;

/**
* A collection of ordered attribute IDs. It is used in definitions of variant indexes.
*/
public final class AttributeIds {
    public final long[] ids;
    public final int hash;

    public AttributeIds(final long[] ids) {
        Arrays.sort(ids);
        this.ids = ids;
        int hash = 17;
        for (int i = 0; i < ids.length; i++) {
            final long id = ids[i];
            hash = (31 * hash) + (int)(id ^ (id >>> 32));
        }
        this.hash = hash;
    }

    public AttributeIds(final Collection<Attribute> attributes) {
        this(idsFromAttributes(attributes));
    }

    public static final long[] idsFromAttributes(final Collection<Attribute> attributes) {
        if ((attributes == null) || (attributes.size() == 0)) return new long[0];
        final long[] result = new long[attributes.size()];
        int i = 0;
        for (final Attribute a : attributes) result[i++] = a.getId();
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (!(obj instanceof AttributeIds)) return false;
        final AttributeIds other = (AttributeIds) obj;
        if (this.ids.length != other.ids.length) return false;
        for (int i = this.ids.length - 1; i >= 0; i--) {
            if (this.ids[i] != other.ids[i]) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(14 + (ids.length * 10)); // a guesstimate
        sb.append("AttributeIds[");
        if ((ids != null) && (ids.length > 0)) {
            sb.append(ids[0]);
            for (int i = 1; i < ids.length; i++) {
                sb.append(", ");
                sb.append(ids[i]);
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
