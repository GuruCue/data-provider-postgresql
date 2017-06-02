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
package com.gurucue.recommendations.data.postgresql;

import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.ProductTypeAttribute;
import com.gurucue.recommendations.entitymanager.ProductTypeAttributeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProductTypeAttributeManagerImpl implements ProductTypeAttributeManager {
    private static final Logger log = LogManager.getLogger(ProductTypeAttributeManagerImpl.class);

    private final PostgreSqlDataLink dataLink;

    ProductTypeAttributeManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public List<ProductTypeAttribute> getProductTypeAttributesForProductType(final Long productTypeId) {
        final List<ProductTypeAttribute> result = new ArrayList<>();
        final ProductType productType = dataLink.provider.productTypeCache.byId(productTypeId);
        try (final PreparedStatement st = dataLink.prepareStatement("select attribute_id, ident_level from product_type_attribute where product_type_id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, productTypeId);
            try (final ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    result.add(new ProductTypeAttribute(productType, dataLink.provider.attributeCache.byId(rs.getLong(1)), rs.getShort(2)));
                }
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to obtain product type attributes for product type with ID " + productTypeId + ": " + e.toString(), e);
        }
        return result;
    }

    @Override
    public List<ProductTypeAttribute> getProductTypeAttributesForProductTypeWithMandatoryIdentLevel(final ProductType productType) {
        final List<ProductTypeAttribute> result = new ArrayList<>();
        try (final PreparedStatement st = dataLink.prepareStatement("select attribute_id, ident_level from product_type_attribute where product_type_id = ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, productType.getId());
            try (final ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    result.add(new ProductTypeAttribute(productType, dataLink.provider.attributeCache.byId(rs.getLong(1)), (short)1));
                }
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to obtain product type attributes for product type with ID " + (productType == null ? "(null)" : productType.getIdentifier()) + ": " + e.toString(), e);
        }
        return result;
    }
}
