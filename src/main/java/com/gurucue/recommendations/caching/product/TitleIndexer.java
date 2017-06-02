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

import com.google.common.collect.ImmutableList;
import com.gurucue.recommendations.caching.MultikeyIndexDefinition;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.value.MultiValue;
import com.gurucue.recommendations.entity.value.TranslatableValue;
import com.gurucue.recommendations.entity.value.Value;

import java.util.Collection;
import java.util.Collections;

/**
 * Indexes products of selected type by their title(s).
 */
public final class TitleIndexer extends MultikeyIndexDefinition<String, Product> {
    private final long selectedProductTypeId;
    private final Attribute titleAttribute;

    public TitleIndexer(final long productTypeId, final Attribute titleAttribute) {
        this.selectedProductTypeId = productTypeId;
        this.titleAttribute = titleAttribute;
    }

    @Override
    public Collection<String> keys(final Product product) {
        if (product == null) return Collections.emptyList();
        if (product.productTypeId != selectedProductTypeId) return Collections.emptyList(); // only index selected product types
        return listAttributeValues(product.attributes.get(titleAttribute));
    }

    protected static ImmutableList<String> listAttributeValues(final Value value) {
        if (value == null) return ImmutableList.of();
        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (value.isArray) {
            final Value[] values = ((MultiValue)value).values;
            if (values.length > 1) {
                final Value example = values[0];
                if (example instanceof TranslatableValue) {
                    for (int i = 0; i < values.length; i++) {
                        final TranslatableValue v = (TranslatableValue) values[i];
                        final String original = v.asString();
                        builder.add(original);
                        for (final String translation : v.translations.values()) {
                            if (!original.equals(translation)) builder.add(translation);
                        }
                    }
                } else {
                    for (int i = 0; i < values.length; i++) builder.add(values[i].asString());
                }
            }
        }
        else {
            if (value instanceof TranslatableValue) {
                final String original = value.asString();
                builder.add(original);
                for (final String translation : ((TranslatableValue)value).translations.values()) {
                    if (!original.equals(translation)) builder.add(translation);
                }
            }
            else {
                builder.add(value.asString());
            }
        }
        return builder.build();
    }
}
