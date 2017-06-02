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
import com.gurucue.recommendations.ResponseException;
import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.blender.DataSet;
import com.gurucue.recommendations.blender.TvChannelData;
import com.gurucue.recommendations.blender.VideoData;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.entity.Attribute;
import com.gurucue.recommendations.entity.Language;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.entity.ProductType;
import com.gurucue.recommendations.entity.product.GeneralVideoProduct;
import com.gurucue.recommendations.entity.product.Matcher;
import com.gurucue.recommendations.entity.product.MatcherKey;
import com.gurucue.recommendations.entity.product.PackageProduct;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.TvChannelProduct;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import com.gurucue.recommendations.entity.product.VideoProduct;
import com.gurucue.recommendations.entity.product.VodProduct;
import com.gurucue.recommendations.entity.value.AttributeValues;
import com.gurucue.recommendations.entity.value.MatchCondition;
import com.gurucue.recommendations.entity.value.TimestampIntervalValue;
import com.gurucue.recommendations.entity.value.TranslatableValue;
import com.gurucue.recommendations.entity.value.Value;
import com.gurucue.recommendations.entitymanager.ProductManager;
import com.gurucue.recommendations.entitymanager.TvChannelInfo;
import com.gurucue.recommendations.entitymanager.TvProgrammeInfo;
import com.gurucue.recommendations.entitymanager.VideoInfo;
import com.gurucue.recommendations.type.ValueType;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Implementation of the Product Manager interface for PostgreSQL.
 */
final class ProductManagerImpl implements ProductManager {
    private static final Logger log = LogManager.getLogger(ProductManagerImpl.class);

    /**
     * Constructs a Product instance from a single ResultSet row.
     * The fields must be given in the following order: id, partner_id, product_type_id, partner_product_code, deleted, added, related, attributes
     *
     * @param utcCalendar a {@link Calendar} instance with UTC time zone, used for proper conversion of timestamps in the database
     * @param rs the {@link ResultSet} instance containing the product data
     * @param logOutput the {@link StringBuilder} instance to which to append any log output
     * @return a {@link Product} instance containing the data in <code>rs</code> after invoking its {@link ResultSet#next()} method
     * @throws SQLException if there was a problem with the database
     * @throws ResponseException if there was a problem processing the data
     */
    private Product productFromDb(final Calendar utcCalendar, final DataProvider provider, final ResultSet rs, final StringBuilder logOutput) throws SQLException, ResponseException {
        if (rs.next()) {
            final long id = rs.getLong(1);
            long partnerId = rs.getLong(2);
            if (rs.wasNull()) partnerId = -1;
            long productTypeId = rs.getLong(3);
            if (rs.wasNull()) productTypeId = -1;
            String productCode = rs.getString(4);
            if (rs.wasNull()) productCode = null;
            Timestamp deleted = rs.getTimestamp(5, utcCalendar);
            if (rs.wasNull()) deleted = null;
            Timestamp modified = rs.getTimestamp(6, utcCalendar);
            if (rs.wasNull()) modified = null;
            Timestamp added = rs.getTimestamp(7, utcCalendar);
            if (rs.wasNull()) added = null;
            String jsonRelated = rs.getString(8);
            if (rs.wasNull()) jsonRelated = null;
            String jsonAttributes = rs.getString(9);
            if (rs.wasNull()) jsonAttributes = null;
            final Product p = Product.create(id, productTypeId, partnerId, productCode, added, modified, deleted, jsonAttributes, jsonRelated, provider, logOutput);
            return p;
        }
        return null;
    }

    /**
     * Constructs Product instances from all ResultSet rows, and returns them in a List.
     * The fields must be given in the following order: id, partner_id, product_type_id, partner_product_code, deleted, added, related, attributes
     *
     * @param utcCalendar a {@link Calendar} instance with UTC time zone, used for proper conversion of timestamps in the database
     * @param rs the {@link ResultSet} instance containing the product data
     * @param logOutput the {@link StringBuilder} instance to which to append any log output
     * @return a list of {@link Product} instances containing the data in <code>rs</code>
     * @throws SQLException if there was a problem with the database
     * @throws ResponseException if there was a problem processing the data
     */
    private List<Product> productsFromDb(final Calendar utcCalendar, final DataProvider provider, final ResultSet rs, final StringBuilder logOutput) throws SQLException, ResponseException {
        final List<Product> list = new ArrayList<>();
        while (rs.next()) {
            final long id = rs.getLong(1);
            long partnerId = rs.getLong(2);
            if (rs.wasNull()) partnerId = -1;
            long productTypeId = rs.getLong(3);
            if (rs.wasNull()) productTypeId = -1;
            String productCode = rs.getString(4);
            if (rs.wasNull()) productCode = null;
            Timestamp deleted = rs.getTimestamp(5, utcCalendar);
            if (rs.wasNull()) deleted = null;
            Timestamp modified = rs.getTimestamp(6, utcCalendar);
            if (rs.wasNull()) modified = null;
            Timestamp added = rs.getTimestamp(7, utcCalendar);
            if (rs.wasNull()) added = null;
            String jsonRelated = rs.getString(8);
            if (rs.wasNull()) jsonRelated = null;
            String jsonAttributes = rs.getString(9);
            if (rs.wasNull()) jsonAttributes = null;
            final Product p = Product.create(id, productTypeId, partnerId, productCode, added, modified, deleted, jsonAttributes, jsonRelated, provider, logOutput);
            list.add(p);
        }
        return list;
    }

    private static final String SQL_GET_PRODUCT_BY_PARTNER_AND_TYPE_AND_CODE = "select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where deleted is null and partner_id = ? and product_type_id = ? and partner_product_code = ?";
    private static final String SQL_GET_PRODUCT_BY_PARTNER_AND_TYPE_AND_CODE_LOCKED = "select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where deleted is null and partner_id = ? and product_type_id = ? and partner_product_code = ? for update";

    @Override
    public Product getProductByPartnerAndTypeAndCode(final Transaction transaction, final Partner partner, final ProductType productType, final String productCode, final boolean locked) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final StringBuilder logOutput = new StringBuilder(256);
        logOutput.append("getProductByPartnerAndTypeAndCode(): ");
        final int initialLogLength = logOutput.length();
        try {
            try (final PreparedStatement st = link.prepareStatement(locked ? SQL_GET_PRODUCT_BY_PARTNER_AND_TYPE_AND_CODE_LOCKED : SQL_GET_PRODUCT_BY_PARTNER_AND_TYPE_AND_CODE)) {
                st.setLong(1, partner.getId());
                st.setLong(2, productType.getId());
                if (productCode == null) st.setNull(3, Types.VARCHAR);
                else st.setString(3, productCode);
                try (final ResultSet rs = st.executeQuery()) {
                    return productFromDb(transaction.getUtcCalendar(), link.getProvider(), rs, logOutput);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } catch (ResponseException e) {
            throw new DatabaseException(e);
        } finally {
            if (logOutput.length() > initialLogLength) {
                log.warn(logOutput.toString());
            }
        }
    }

    private static final String SQL_GET_BY_ID = "select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where id = ?";
    private static final String SQL_GET_BY_ID_LOCKED = "select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where id = ? for update";

    @Override
    public Product getById(final Transaction transaction, final Partner partner, final long productId, final boolean locked) {
        // the partner hint is ignored, because we have no internal segmentation of products on the physical level
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final StringBuilder logOutput = new StringBuilder(256);
        logOutput.append("getById(): ");
        final int initialLogLength = logOutput.length();
        try {
            try (final PreparedStatement st = link.prepareStatement(locked ? SQL_GET_BY_ID_LOCKED : SQL_GET_BY_ID)) {
                st.setLong(1, productId);
                try (final ResultSet rs = st.executeQuery()) {
                    return productFromDb(transaction.getUtcCalendar(), link.getProvider(), rs, logOutput);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } catch (ResponseException e) {
            throw new DatabaseException(e);
        } finally {
            if (logOutput.length() > initialLogLength) {
                log.warn(logOutput.toString());
            }
        }
    }

/*
create function json_array_elements_text_guru(json) returns setof text immutable language sql as
$$ select json->>i from generate_series(0, json_array_length(json)-1) i; $$;
*/

    @Override
    public List<Product> findProductsHavingAttributes(final Transaction transaction, final ProductType productType, final Partner partner, final Map<Attribute, Value> attributeValues) {
        if (productType == null) throw new DatabaseException("productType is null");
        if (productType.getId() == null) throw new DatabaseException("productType.id is null");
        if (partner == null) throw new DatabaseException("partner is null");
        if (partner.getId() == null) throw new DatabaseException("partner.id is null");
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final List<Product> result = new ArrayList<>();
        final StringBuilder selectorSql = new StringBuilder(300);
        final StringBuilder conditionSql = new StringBuilder(200);
        final ArrayList<Object> conditionParameters = new ArrayList<>();
        selectorSql.append("select distinct p.id, p.partner_product_code, p.added, p.modified, p.deleted, p.attributes::text, p.related::text from product p");
        conditionSql.append(" where p.deleted is null and p.partner_id = ");
        conditionSql.append(partner.getId());
        conditionSql.append(" and p.product_type_id = ?");
        int parameterCounter = 1;
        conditionParameters.add(productType.getId());
        if (attributeValues != null) {
            for (final Map.Entry<Attribute, Value> pa : attributeValues.entrySet()) {
                final Attribute a = pa.getKey();
                parameterCounter++;

                if (a.getIsMultivalue()) {
                    if (a.getIsTranslatable()) {
                        selectorSql.append(" left join jsonb_array_elements(p.attributes->'");
                        selectorSql.append(a.getIdentifier());
                        selectorSql.append("') pat");
                        selectorSql.append(parameterCounter);
                        selectorSql.append(" on TRUE");
                        selectorSql.append(" left join jsonb_each(pat");
                        selectorSql.append(parameterCounter);
                        selectorSql.append(".value->'translations') pa");
                        selectorSql.append(parameterCounter);
                        selectorSql.append(" on TRUE");
                        addTranslatableValueConditions(provider, parameterCounter, pa.getValue().asTranslatable(), conditionSql, conditionParameters);
                    }
                    else {
                        if (a.getValueType() == ValueType.TIMESTAMP_INTERVAL) {
                            // complex value extraction - json must be the result, for further extraction
                            selectorSql.append(" left join jsonb_array_elements(p.attributes->'");
                            selectorSql.append(a.getIdentifier());
                            selectorSql.append("') pa");
                            selectorSql.append(parameterCounter);
                            selectorSql.append(" on TRUE");
                            final TimestampIntervalValue v = pa.getValue().asTimestampInterval();
                            conditionSql.append(" and (pa").append(parameterCounter).append(".value->>'begin')::bigint = ?");
                            conditionParameters.add(Long.valueOf(v.beginMillis / 1000L));
                            conditionSql.append(" and (pa").append(parameterCounter).append(".value->>'end')::bigint = ?");
                            conditionParameters.add(Long.valueOf(v.endMillis / 1000L));
                        }
                        else {
                            // primitive value extraction - text must be the result
                            // ugly hack because postgresql before 9.4 lacks json_array_elements_text
                            //   but then, we want to use the "#>>" operator, because a cascading "->" throws an error if a value in the cascade is missing
                            selectorSql.append(" left join generate_series(0, jsonb_array_length(p.attributes->'").append(a.getIdentifier()).append("')-1) pa").append(parameterCounter).append(" on TRUE");
                            addPrimitiveValueConditions("(p.attributes#>>('{" + a.getIdentifier() + ",' || pa" + parameterCounter + " || '}')::text[])", a.getValueType(), pa.getValue(), conditionSql, conditionParameters);
                        }
                    }
                }
                else {
                    if (a.getIsTranslatable()) {
                        selectorSql.append(" left join json_each(p.attributes#>'{");
                        selectorSql.append(a.getIdentifier());
                        selectorSql.append(",translations}') pa");
                        selectorSql.append(parameterCounter);
                        selectorSql.append(" on TRUE");
                        addTranslatableValueConditions(provider, parameterCounter, pa.getValue().asTranslatable(), conditionSql, conditionParameters);
                    }
                    else if (a.getValueType() == ValueType.TIMESTAMP_INTERVAL) {
                        final TimestampIntervalValue v = pa.getValue().asTimestampInterval();
                        conditionSql.append(" and (p.attributes#>>'{").append(a.getIdentifier()).append(",begin}')::bigint = ?");
                        conditionParameters.add(Long.valueOf(v.beginMillis / 1000L));
                        conditionSql.append(" and (p.attributes#>>'{").append(a.getIdentifier()).append(",end}')::bigint = ?");
                        conditionParameters.add(Long.valueOf(v.endMillis / 1000L));
                    }
                    else {
                        addPrimitiveValueConditions("(p.attributes->>'" + a.getIdentifier() + "')", a.getValueType(), pa.getValue(), conditionSql, conditionParameters);
                    }
                }
            }
        }
        selectorSql.append(conditionSql);
        final String sql = selectorSql.toString();
        selectorSql.insert(0, "] Preparing SQL:\n");
        selectorSql.insert(0, Thread.currentThread().getId());
        selectorSql.insert(0, "[Thread ");
        log.info(selectorSql.toString());

        try (final PreparedStatement st = link.prepareStatement(sql)) {
            int columnIndex = 1;
            final StringBuilder paramLog = new StringBuilder();
            for (final Object parameter : conditionParameters) {
                paramLog.append(", ");
                if (parameter == null) throw new DatabaseException("NULL parameters not allowed (column index " + columnIndex + ")");
                if (parameter instanceof Long) {
                    st.setLong(columnIndex, (Long)parameter);
                    paramLog.append(parameter.toString());
                }
                else if (parameter instanceof String) {
                    final String param = (String) parameter;
                    st.setString(columnIndex, param);
                    paramLog.append(param);
                }
                else if (parameter instanceof Boolean) {
                    final Boolean param = (Boolean) parameter;
                    st.setBoolean(columnIndex, param);
                    paramLog.append(param ? "TRUE" : "FALSE");
                }
                else if (parameter instanceof Double) {
                    final Double param = (Double) parameter;
                    st.setDouble(columnIndex, param);
                    paramLog.append(param);
                }
                else throw new DatabaseException("Don't know how to set SQL parameter of type " + parameter.getClass().getCanonicalName() + " at column index " + columnIndex);
                columnIndex++;
            }
            paramLog.delete(0, 2); // remove the first ", "
            paramLog.insert(0, "SQL parameters: ");
            log.info(paramLog.toString());
            final long productTypeId = productType.getId().longValue();
            final long partnerId = partner.getId().longValue();
            final StringBuilder productInstantiationLog = new StringBuilder(50);
            final Calendar cal = transaction.getUtcCalendar();
            try (final ResultSet rs = st.executeQuery()) {
                while (rs.next()) { // TODO: use productsFromDb() instead of code below
                    final long productId = rs.getLong(1);
                    String code = rs.getString(2);
                    if (rs.wasNull()) code = null;
                    Timestamp added = rs.getTimestamp(3, cal);
                    if (rs.wasNull()) added = null;
                    Timestamp modified = rs.getTimestamp(4, cal);
                    if (rs.wasNull()) modified = null;
                    Timestamp deleted = rs.getTimestamp(5, cal);
                    if (rs.wasNull()) deleted = null;
                    String attributes = rs.getString(6);
                    if (rs.wasNull()) attributes = null;
                    String related = rs.getString(7);
                    if (rs.wasNull()) related = null;
                    try {
                        result.add(Product.create(productId, productTypeId, partnerId, code, added, modified, deleted, attributes, related, provider, productInstantiationLog));
                        if (productInstantiationLog.length() > 0) {
                            productInstantiationLog.insert(0, "Errors while instantiating product with ID " + productId + ": ");
                            log.warn(productInstantiationLog.toString());
                            productInstantiationLog.delete(0, productInstantiationLog.length());
                        }
                    }
                    catch (ResponseException e) {
                        log.error("Failed to instantiated product with ID " + productId + " (ignoring): " + e.toString(), e);
                    }
                }
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to search for a product with given attributes: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }

        return result;
    }

    private void addTranslatableValueConditions(
            final DataProvider provider,
            final int parameterCounter,
            final TranslatableValue translatableValue,
            final StringBuilder conditionSql,
            final ArrayList<Object> conditionParameters
    ) {
        conditionSql.append(" and (");
        final Iterator<Map.Entry<Language, String>> translationIterator = translatableValue.translations.entrySet().iterator();
        boolean originalLanguageIncluded = false; // a safety, in case for some reason the given value is not per specs (i.e. default language/value must be present among translations)
        if (translationIterator.hasNext()) {
            Map.Entry<Language, String> entry = translationIterator.next();
            addTranslationCondition(provider, parameterCounter, entry.getKey(), entry.getValue(), conditionSql, conditionParameters);
            originalLanguageIncluded = translatableValue.language.equals(entry.getKey());
            while (translationIterator.hasNext()) {
                entry = translationIterator.next();
                conditionSql.append(" or ");
                addTranslationCondition(provider, parameterCounter, entry.getKey(), entry.getValue(), conditionSql, conditionParameters);
                if (!originalLanguageIncluded) {
                    originalLanguageIncluded = translatableValue.language.equals(entry.getKey());
                }
            }
        }
        if (!originalLanguageIncluded) {
            // also include the test on the original language/value
            if (translatableValue.translations.size() > 0) conditionSql.append(" or ");
            addTranslationCondition(provider, parameterCounter, translatableValue.language, translatableValue.value, conditionSql, conditionParameters);
        }
        conditionSql.append(")");
    }

    private void addTranslationCondition(
            final DataProvider provider,
            final int parameterCounter,
            final Language translationLanguage,
            final String translationValue,
            final StringBuilder conditionSql,
            final ArrayList<Object> conditionParameters
    ) {
        if (translationLanguage.getId().longValue() == provider.getLanguageCodes().idForUknown) {
            // only test the value
            conditionSql.append("pa");
            conditionSql.append(parameterCounter);
            conditionSql.append(".value = ?");
            conditionParameters.add(translationValue);
        }
        else {
            conditionSql.append("(pa");
            conditionSql.append(parameterCounter);
            conditionSql.append(".value = ?");
            conditionParameters.add(translationValue);
            conditionSql.append(" and (pa");
            conditionSql.append(parameterCounter);
            conditionSql.append(".key = ? or pa");
            conditionParameters.add(translationLanguage.getIso639_2t());
            conditionSql.append(parameterCounter);
            conditionSql.append(".key = ?))");
            conditionParameters.add(Language.UNKNOWN);
        }
    }

    private void addPrimitiveValueConditions(
            final String fieldHandle,
            final ValueType valueType,
            final Value value,
            final StringBuilder conditionSql,
            final ArrayList<Object> conditionParameters
    ) {
        switch (valueType) {
            case INTEGER:
                conditionSql.append(" and ").append(fieldHandle).append("::text::bigint = ?");
                conditionParameters.add(Long.valueOf(value.asInteger()));
                break;
            case BOOLEAN:
                conditionSql.append(" and ").append(fieldHandle).append("::text::bool = ?");
                conditionParameters.add(value.asBoolean() ? Boolean.TRUE : Boolean.FALSE);
                break;
            case FLOAT:
                conditionSql.append(" and ").append(fieldHandle).append("::text::double precision = ?");
                conditionParameters.add(Double.valueOf(value.asFloat()));
                break;
            case TIMESTAMP:
                conditionSql.append(" and ").append(fieldHandle).append("::text::bigint = ?");
                conditionParameters.add(Long.valueOf(value.asInteger() / 1000L));
                break;
            case STRING:
                conditionSql.append(" and ").append(fieldHandle).append("::text = ?");
                conditionParameters.add(value.asString());
                break;
            default:
                throw new DatabaseException("Attempted to add a primitive condition for a value of complex type " + valueType);
        }
    }

/*
update product
set deleted = current_timestamp at time zone 'utc'
where partner_id = 7 and product_type_id = 3 and deleted is null and id != 23421 and ('"MGM"'::jsonb) <@ (attributes->'tv-channel')
and (((attributes->>'begin-time')::bigint >= 1408053600 and (attributes->>'begin-time')::bigint < 1408140000)
  or ((attributes->>'end-time')::bigint <= 1408140000 and (attributes->>'end-time')::bigint > 1408053600)
  or ((attributes->>'begin-time')::bigint <= 1408053600 and (attributes->>'end-time')::bigint >= 1408140000))
returning id;

select * from product
where partner_id = 6 and product_type_id = 3 and deleted is null and id != 23421 and ('"MGM"'::jsonb) <@ (attributes->'tv-channel')
and (((attributes->>'begin-time')::bigint >= 1408053600 and (attributes->>'begin-time')::bigint < 1408140000)
  or ((attributes->>'end-time')::bigint <= 1408140000 and (attributes->>'end-time')::bigint > 1408053600)
  or ((attributes->>'begin-time')::bigint <= 1408053600 and (attributes->>'end-time')::bigint >= 1408140000));
*/
// TODO: do not remove Product instances that overlap, instead just remove the tv-channel code from their attributes,
// TODO: so we gain the proper multi-tv-channel support for tv-programmes
    private static final String removeTvOverlappingSql = new StringBuilder(500)
            .append("update product set deleted = ? where partner_id = ? and product_type_id = ? and deleted is null and id != ? and ? <@ (attributes->'") // params: deleted, partner_id, product_type_id, excludedId, tv-channel code
            .append(Attribute.TV_CHANNEL)
            .append("') and (((attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("')::bigint >= ? and (attributes->>'") // param: begin-time
            .append(Attribute.BEGIN_TIME)
            .append("')::bigint < ?) or ((attributes->>'") // param: end-time
            .append(Attribute.END_TIME)
            .append("')::bigint <= ? and (attributes->>'") // param: end-time
            .append(Attribute.END_TIME)
            .append("')::bigint > ?) or ((attributes->>'") // param: begin-time
            .append(Attribute.BEGIN_TIME)
            .append("')::bigint <= ? and (attributes->>'") // param: begin-time
            .append(Attribute.END_TIME)
            .append("')::bigint >= ?)) returning id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text") // param: end-time: TODO: returning everything, and then constructing Product instances
            .toString();

    // create extension btree_gin;
    // create index tv_programme_begintime_channel_idx1 on product using gin (partner_id, cast(attributes->>'begin-time' as bigint), (attributes->'tv-channel')) where product_type_id = 3;

    @Override
    public List<Product> removeTvProgrammesOverlappingInterval(final Transaction transaction, final TvProgrammeProduct tvProgramme) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final List<Product> removedProducts = new ArrayList<>();
        final Timestamp now = new Timestamp(Timer.currentTimeMillis());
        final StringBuilder constructionLog = new StringBuilder(400);
        constructionLog.append("removeTvProgrammesOverlappingInterval(): ");
        final int initialConstructionLogLength = constructionLog.length();
        final long beginTime = tvProgramme.beginTimeMillis;
        final long endTime = tvProgramme.endTimeMillis;
        final Calendar utcCalendar = transaction.getUtcCalendar();
        try (final PreparedStatement st = link.prepareStatement(removeTvOverlappingSql)) {
            for (final String tvChannelId : tvProgramme.tvChannelCodes) {
                st.setTimestamp(1, now, utcCalendar);
                st.setLong(2, tvProgramme.partnerId);
                st.setLong(3, provider.getProductTypeCodes().idForTvProgramme);
                st.setLong(4, tvProgramme.id);
                final PGobject jsonTvChannel = new PGobject();
                jsonTvChannel.setType("jsonb");
                jsonTvChannel.setValue("\"" + tvChannelId + "\"");
                st.setObject(5, jsonTvChannel);
                st.setLong(6, beginTime);
                st.setLong(7, endTime);
                st.setLong(8, endTime);
                st.setLong(9, beginTime);
                st.setLong(10, beginTime);
                st.setLong(11, endTime);
                try (final ResultSet rs = st.executeQuery()) {
                    removedProducts.addAll(productsFromDb(utcCalendar, provider, rs, constructionLog));
                }
            }
        }
        catch(SQLException e){
            final String reason = "Failed to run the query to get overlapping TV shows: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
        catch(ResponseException e){
            final String reason = "Failed to construct products of overlapping TV shows: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
        finally{
            if (constructionLog.length() > initialConstructionLogLength) {
                log.warn(constructionLog.toString());
            }
        }

        // optimize for the case where debug is disabled
        if (log.isDebugEnabled()) {
            final StringBuilder logOutput = new StringBuilder((removedProducts.size() * 10) + 300); // guesstimate
            if (removedProducts.size() > 0) {
                logOutput.append("[Thread ")
                        .append(Thread.currentThread().getId())
                        .append("] Marked ")
                        .append(removedProducts.size())
                        .append(" overlapped tv-programme products for partner ")
                        .append(tvProgramme.partnerId)
                        .append(" on tv-channel(s) ");
                int i = tvProgramme.tvChannelCodes.length - 1;
                if (i < 0) logOutput.append("[no channels defined]");
                else {
                    logOutput.append(tvProgramme.tvChannelCodes[i]);
                    i--;
                    while (i >= 0) {
                        logOutput.append(", ");
                        logOutput.append(tvProgramme.tvChannelCodes[i]);
                        i--;
                    }
                }
                logOutput.append(" between ")
                        .append(beginTime)
                        .append(" and ")
                        .append(endTime)
                        .append(" as deleted, and private products referencing them, with IDs: ");
                final Iterator<Product> it = removedProducts.iterator();
                if (it.hasNext()) {
                    logOutput.append(it.next().id);
                    while (it.hasNext()) {
                        logOutput.append(", ").append(it.next().id);
                    }
                }
            } else {
                logOutput.append("[Thread ")
                        .append(Thread.currentThread().getId())
                        .append("] No tv-programmes overlapped for partner ")
                        .append(tvProgramme.partnerId)
                        .append(" on tv channel(s) ");
                int i = tvProgramme.tvChannelCodes.length - 1;
                if (i < 0) logOutput.append("[no channels defined]");
                else {
                    logOutput.append(tvProgramme.tvChannelCodes[i]);
                    i--;
                    while (i >= 0) {
                        logOutput.append(", ");
                        logOutput.append(tvProgramme.tvChannelCodes[i]);
                        i--;
                    }
                }
                logOutput.append(" between ")
                        .append(beginTime)
                        .append(" and ")
                        .append(endTime)
                        .append(", none marked as deleted");
            }
            log.debug(logOutput.toString());
        }

        return removedProducts;
    }

/*
select id, partner_product_code, attributes::text, related::text
from product
where partner_id = 6 and product_type_id = 3 and deleted is null
and (
  (cast(attributes->>'begin-time' as bigint) >= 1397745240 and cast(attributes->>'begin-time' as bigint) <= 1397752440) or -- min, max
  (cast(attributes->>'begin-time' as bigint) < 1397748840 and cast(attributes->>'end-time' as bigint) > 1397748840)) -- now
*/
    private static final String tvProgrammeInIntervalSql = new StringBuilder(500)
            .append("select id, partner_product_code, added, modified, attributes::text, related::text from product where partner_id = ? and product_type_id = ? and deleted is null and ((cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) >= ? and cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) <= ?) or (cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) < ? and cast(attributes->>'")
            .append(Attribute.END_TIME)
            .append("' as bigint) > ?))")
            .toString();

    @Override
    public List<TvProgrammeInfo> tvProgrammesInIntervalForPartner(final Transaction transaction, final Partner partner, final long recommendTimeMillis, final long negativeOffsetMillis, final long positiveOffsetMillis) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();

        final Map<String, List<PackageProduct>> packagesMap = new HashMap<>(); // tv-channel code -> list of PackageProduct instances
        for (final PackageProduct partnerPackage : listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
            for (final String code : partnerPackage.tvChannelCodes) {
                final List<PackageProduct> existingPackageList = packagesMap.get(code);
                if (existingPackageList == null) {
                    final List<PackageProduct> l = new ArrayList<>();
                    l.add(partnerPackage);
                    packagesMap.put(code, l);
                }
                else {
                    existingPackageList.add(partnerPackage);
                }
            }
        }

        final Map<String, TvChannelInfo> tvChannelInfos = new HashMap<>();
        for (final TvChannelProduct product : listByType(transaction, partner, productTypeCodes.tvChannel, TvChannelProduct.class)) {
            final String tvChannelCode = product.partnerProductCode;
            final List<PackageProduct> packageList = packagesMap.get(tvChannelCode);
            if (packageList == null) continue; // tv-channel not in any package
            tvChannelInfos.put(tvChannelCode, new TvChannelInfo(product, packageList));
        }

/*
select pr.id, pr.partner_product_code, pr.product_id
from product pr
inner join product pu on pu.id = pr.product_id
inner join product_attribute b on b.product_id = pu.id and b.attribute_id = 56
inner join product_attribute e on e.product_id = pu.id and e.attribute_id = 57
where pr.partner_id = 6 and pr.product_type_id = 3 and pr.deleted is null
and (
  (cast(b.original_value as bigint) >= 1397745240 and cast(b.original_value as bigint) <= 1397752440) or -- min, max
  (cast(b.original_value as bigint) < 1397748840 and cast(e.original_value as bigint) > 1397748840)) -- now

select id, partner_product_code, attributes::text, related::text
from product
where partner_id = 6 and product_type_id = 3 and deleted is null
and (
  (cast(attributes->>'begin-time' as bigint) >= 1397745240 and cast(attributes->>'begin-time' as bigint) <= 1397752440) or -- min, max
  (cast(attributes->>'begin-time' as bigint) < 1397748840 and cast(attributes->>'end-time' as bigint) > 1397748840)) -- now

*/
        final long minTime = recommendTimeMillis - negativeOffsetMillis;
        final long maxTime = recommendTimeMillis + positiveOffsetMillis;
        final List<TvProgrammeProduct> tvProgrammeProducts;
        try (final PreparedStatement st = link.prepareStatement(tvProgrammeInIntervalSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvProgramme);
            st.setLong(3, minTime / 1000L);
            st.setLong(4, maxTime / 1000L);
            st.setLong(5, recommendTimeMillis / 1000L);
            st.setLong(6, recommendTimeMillis / 1000L);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                tvProgrammeProducts = readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForTvProgramme, rs, TvProgrammeProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list tv-programmes: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("Obtained " + tvProgrammeProducts.size() + " tv-programmes in the range [" + recommendTimeMillis + "-" + negativeOffsetMillis + ", " + recommendTimeMillis + "+" + positiveOffsetMillis + "]");

        int videoIdMisses = 0;
        int tvChannelMisses = 0;
        int catchupMisses = 0;
        int tvProgrammeMisses = 0;
        final List<TvProgrammeInfo> result = new ArrayList<>();
        for (final TvProgrammeProduct tvProgramme : tvProgrammeProducts) {
            if (tvProgramme.videoMatchId < 0L) { // no such video
                videoIdMisses++;
                continue;
            }

            List<TvChannelInfo> reachableThroughTvChannels = null;
            final int tvChannelCount = tvProgramme.tvChannelCodes.length;
            for (int i = 0; i < tvChannelCount; i++) {
                final TvChannelInfo tvChannelInfo = tvChannelInfos.get(tvProgramme.tvChannelCodes[i]);
                if (tvChannelInfo == null) { // no such TV-channel
                    tvChannelMisses++;
                    continue;
                }
                if ((tvProgramme.beginTimeMillis < (recommendTimeMillis - tvChannelInfo.tvChannel.catchupMillis)) && (tvProgramme.endTimeMillis < recommendTimeMillis)) { // out of catch-up range, and not currently running
                    catchupMisses++;
                    continue;
                }
                if (reachableThroughTvChannels == null) reachableThroughTvChannels = new ArrayList<>();
                reachableThroughTvChannels.add(tvChannelInfo);
            }
            if (reachableThroughTvChannels == null) { // no TV-channel
                tvProgrammeMisses++;
                continue;
            }

            result.add(new TvProgrammeInfo(
                    tvProgramme,
                    reachableThroughTvChannels
            ));
        }
        log.debug("After verifications there are " + result.size() + " tv-programmes remaining: " + videoIdMisses + " had missing video-id, " + tvChannelMisses + " referenced a non-existent tv-channel code, " + catchupMisses + " were not inside catch-up for their channel");

        return result;
    }

/*
select distinct p.id, p.partner_product_code from product p inner join json_array_elements(p.attributes->'validity') v on TRUE where p.partner_id = 6 and p.product_type_id = 1 and p.deleted is null and cast(v.value->>'begin' as bigint) <= 1408540615 and cast(v.value->>'end' as bigint) > 1408540615;
*/
    /** Parameters: partnerId, productTypeId, recommendTime, recommendTime */
    private static final String vodForPartnerSql = new StringBuilder(500)
            .append("select distinct p.id, p.partner_product_code, p.added, p.modified, p.attributes::text, p.related::text from product p inner join jsonb_array_elements(p.attributes->'")
            .append(Attribute.VALIDITY)
            .append("') v on TRUE where p.partner_id = ? and p.product_type_id = ? and p.deleted is null and cast(v.value->>'begin' as bigint) <= ? and cast(v.value->>'end' as bigint) > ?")
            .toString();

    @Override
    public List<VideoInfo> vodForPartner(final Transaction transaction, final Partner partner, final long recommendTimeMillis) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        final Map<String, VodProduct> vodCatalogues = new HashMap<>();
        for (final VodProduct vod : listByType(transaction, partner, productTypeCodes.svod, VodProduct.class)) {
            vodCatalogues.put(vod.catalogueId, vod);
        }
        for (final VodProduct vod : listByType(transaction, partner, productTypeCodes.tvod, VodProduct.class)) {
            vodCatalogues.put(vod.catalogueId, vod);
        }

        final Map<String, List<PackageProduct>> packagesMap = new HashMap<>();
        for (final PackageProduct partnerPackage : listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
            for (final String code : partnerPackage.vodCodes) {
                try {
                    packagesMap.get(code).add(partnerPackage);
                }
                catch (NullPointerException e) {
                    final List<PackageProduct> l = new ArrayList<>();
                    l.add(partnerPackage);
                    packagesMap.put(code, l);
                }
            }
        }

/*
select pr.id, pr.partner_product_code, pr.product_id
from product pr
inner join log_product l on l.log_id = pr.last_log_id
where pr.partner_id = 6 and pr.product_type_id = 1 and pr.deleted is null
and l.valid_from <= '2014-01-15'
*/
        final List<VideoProduct> videoProducts;
        try (final PreparedStatement st = link.prepareStatement(vodForPartnerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForVideo);
            st.setLong(3, recommendTimeMillis / 1000L);
            st.setLong(4, recommendTimeMillis / 1000L);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                videoProducts = readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForVideo, rs, VideoProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list VOD videos: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("Obtained " + videoProducts.size() + " VOD products for recommendations at time " + recommendTimeMillis);

        final List<VideoInfo> result = new ArrayList<>();
        int catalogueMisses = 0;
        for (final VideoProduct video : videoProducts) {
            final VodProduct catalogue = vodCatalogues.get(video.catalogueId);
            if (catalogue == null) { // no such catalogue
                catalogueMisses++;
                continue;
            }
            final List<PackageProduct> packages = packagesMap.get(catalogue.partnerProductCode);
            result.add(new VideoInfo(
                    video,
                    catalogue,
                    packages == null ? Collections.<PackageProduct>emptyList() : packages
            ));
        }
        log.debug("After verifications there are " + result.size() + " VODs remaining: " + /*expiredMisses + " expired, " +*/ catalogueMisses + " referenced a non-existent VOD catalogue");

        return result;
    }

/*
select id, partner_product_code
from product
where partner_id = 6
and product_type_id = 3
and deleted is null
and '"SLO1"'::jsonb <@ (attributes->'tv-channel')
and cast(attributes->>'begin-time' as bigint) <= 1400668880
and cast(attributes->>'end-time' as bigint) >= 1400668880;
*/
    private static final String getSelectTvProgrammeAtTimeForTvChannelAndPartnerSql = new StringBuilder(500)
            .append("select id, partner_product_code, added, modified, attributes::text, related::text from product where partner_id = ? and product_type_id = ? and deleted is null and ? <@ (attributes->'")
            .append(Attribute.TV_CHANNEL)
            .append("') and cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) <= ? and cast(attributes->>'")
            .append(Attribute.END_TIME)
            .append("' as bigint) >= ?")
            .toString();

    @Override
    public TvProgrammeProduct tvProgrammeAtTimeForTvChannelAndPartner(final Transaction transaction, final Partner partner, final TvChannelProduct tvChannel, final long timeMillis) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        final long timeSeconds = timeMillis / 1000;
        long productId = 0L;
        try (final PreparedStatement st = link.prepareStatement(getSelectTvProgrammeAtTimeForTvChannelAndPartnerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvProgramme);
            final PGobject jsonTvChannel = new PGobject();
            jsonTvChannel.setType("jsonb");
            jsonTvChannel.setValue("\"" + tvChannel.partnerProductCode.replace("\"", "\\\"") + "\"");
            st.setObject(3, jsonTvChannel);
            st.setLong(4, timeSeconds);
            st.setLong(5, timeSeconds);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                productId = rs.getLong(1);
                final StringBuilder logBuilder = new StringBuilder(100);
                final TvProgrammeProduct p = readProduct(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForTvProgramme, null, rs, logBuilder, TvProgrammeProduct.class);
                if (logBuilder.length() > 0) {
                    log.error("[Thread " + Thread.currentThread().getId() + "] Errors while instantiating a Product with ID " + p.id + " (ignored):\n" + logBuilder.toString());
                }
                return p;
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to find a TV-programme active at " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ResponseException e) {
            final String reason = "Failed to find a TV-programme active at " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": failed to instantiate a Product with ID " + productId + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ClassCastException e) {
            final String reason = "Failed to find a TV-programme active at " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": failed to cast a Product instance with ID " + productId + " to " + TvProgrammeProduct.class.getCanonicalName() + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

/*
select id, partner_product_code
from product
where partner_id = 6
and product_type_id = 3
and deleted is null
and '"SLO1"'::jsonb <@ (attributes->'tv-channel')
and cast(attributes->>'begin-time' as bigint) > 1400668880
order by cast(attributes->>'begin-time' as bigint) desc
limit 1;
*/

    private static final String firstTvProgrammeAfterTimeForTvChannelAndPartnerSql = new StringBuilder(500)
            .append("select id, partner_product_code, added, modified, attributes::text, related::text from product where partner_id = ? and product_type_id = ? and deleted is null and ? <@ (attributes->'")
            .append(Attribute.TV_CHANNEL)
            .append("') and cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) > ? order by cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) desc limit 1")
            .toString();

    @Override
    public TvProgrammeProduct firstTvProgrammeAfterTimeForTvChannelAndPartner(final Transaction transaction, final Partner partner, final TvChannelProduct tvChannel, final long timeMillis) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        final long timeSeconds = timeMillis / 1000;
        long productId = 0L;
        try (final PreparedStatement st = link.prepareStatement(firstTvProgrammeAfterTimeForTvChannelAndPartnerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvProgramme);
            final PGobject jsonTvChannel = new PGobject();
            jsonTvChannel.setType("jsonb");
            jsonTvChannel.setValue("\"" + tvChannel.partnerProductCode.replace("\"", "\\\"") + "\"");
            st.setObject(3, jsonTvChannel);
            st.setLong(4, timeSeconds);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                productId = rs.getLong(1);
                final StringBuilder logBuilder = new StringBuilder(100);
                final TvProgrammeProduct p = readProduct(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForTvProgramme, null, rs, logBuilder, TvProgrammeProduct.class);
                if (logBuilder.length() > 0) {
                    log.error("[Thread " + Thread.currentThread().getId() + "] Errors while instantiating a Product with ID " + p.id + " (ignored):\n" + logBuilder.toString());
                }
                return p;
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to find the first TV-programme active after " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ResponseException e) {
            final String reason = "Failed to find the first TV-programme active after " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": failed to instantiate a Product with ID " + productId + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ClassCastException e) {
            final String reason = "Failed to find the first TV-programme active after " + timeSeconds + " on TV-channel " + tvChannel.partnerProductCode + ": failed to cast a Product instance with ID " + productId + " to " + TvProgrammeProduct.class.getCanonicalName() + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

/*
select id, partner_product_code, added, attributes::text, related::text, product_type_id
from product
where deleted is null
and partner_id = 6
and product_type_id in (5,6)
and '"7"'::jsonb <@ (attributes->'catalogue-id');
*/

    private static final String getVodCatalogueSql = new StringBuilder(200)
            .append("select id, partner_product_code, added, modified, attributes::text, related::text, product_type_id from product where deleted is null and partner_id = ? and product_type_id in (?,?) and ? <@ (attributes->'")
            .append(Attribute.CATALOGUE_ID)
            .append("')")
            .toString();

    /**
     * Returns a product of type SVOD or TVOD catalogue that haven't been deleted yet.
     * @param partner
     * @param catalogueId
     * @return
     */
    @Override
    public VodProduct getVodCatalogue(final Transaction transaction, final Partner partner, final String catalogueId) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        long productId = 0L;
        try (final PreparedStatement st = link.prepareStatement(getVodCatalogueSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvod);
            st.setLong(3, productTypeCodes.idForSvod);
            final PGobject jsonCatalogueId = new PGobject();
            jsonCatalogueId.setType("jsonb");
            jsonCatalogueId.setValue("\"" + catalogueId.replace("\"", "\\\"") + "\"");
            st.setObject(4, jsonCatalogueId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                productId = rs.getLong(1);
                final StringBuilder logBuilder = new StringBuilder(100);
                final VodProduct p = readProduct(transaction.getUtcCalendar(), provider, partner.getId(), rs.getLong(7), null, rs, logBuilder, VodProduct.class);
                if (logBuilder.length() > 0) {
                    log.error("[Thread " + Thread.currentThread().getId() + "] Errors while instantiating a Product with ID " + p.id + " (ignored):\n" + logBuilder.toString());
                }
                return p;
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to obtain a VOD catalogue with code " + catalogueId + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ResponseException e) {
            final String reason = "Failed to obtain a VOD catalogue with code " + catalogueId + ": failed to instantiate a Product with ID " + productId + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ClassCastException e) {
            final String reason = "Failed to obtain a VOD catalogue with code " + catalogueId + ": failed to cast a Product instance with ID " + productId + " to " + VodProduct.class.getCanonicalName() + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

/*
select p.id, p.partner_product_code, p.added, p.attributes::text, p.related::text
from product p
inner join product c on c.partner_id = p.partner_id and c.product_type_id in (5,6) and ('"' || c.partner_product_code || '"')::jsonb <@ (p.attributes->'svod-id')
where p.deleted is null and p.partner_id = 6
and p.product_type_id = 2
and '"7"'::jsonb <@ (c.attributes->'catalogue-id');
*/

    private static final String getPackagesForCatalogueSql = new StringBuilder(500)
            .append("select p.id, p.partner_product_code, p.added, p.modified, p.attributes::text, p.related::text from product p ")
            .append("inner join product c on c.partner_id = p.partner_id and c.product_type_id in (?,?) and ('\"' || c.partner_product_code || '\"')::jsonb <@ (p.attributes->'")
            .append(Attribute.SVOD_ID)
            .append("') where p.deleted is null and p.partner_id = ? and p.product_type_id = ? and ? <@ c.attributes->'")
            .append(Attribute.CATALOGUE_ID)
            .append("')")
            .toString();

    /**
     * Returns products of type package that haven't been deleted yet,
     * that contain subscription to the specified VOD catalogue.
     * @param partner
     * @param catalogueId
     * @return
     */
    @Override
    public List<PackageProduct> getPackagesForCatalogue(final Transaction transaction, final Partner partner, final String catalogueId) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        try (final PreparedStatement st = link.prepareStatement(getPackagesForCatalogueSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, productTypeCodes.idForTvod);
            st.setLong(2, productTypeCodes.idForSvod);
            st.setLong(3, partner.getId());
            st.setLong(4, productTypeCodes.idForPackage);
            final PGobject jsonCatalogueId = new PGobject();
            jsonCatalogueId.setType("jsonb");
            jsonCatalogueId.setValue("\"" + catalogueId.replace("\"", "\\\"") + "\"");
            st.setObject(5, jsonCatalogueId);
            try (final ResultSet rs = st.executeQuery()) {
                return readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForPackage, rs, PackageProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to obtain a list of packages for a VOD catalogue: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

/*
select p.id, p.partner_product_code, p.added, p.modified, p.attributes::text, p.related::text
from product p
where p.deleted is null and p.partner_id = 6
and p.product_type_id = 2
and '"MTV"'::jsonb <@ (p.attributes->'tv-channel-id');
*/
    private static final String getPackagesForTvChannelSql = new StringBuilder(500)
        .append("select p.id, p.partner_product_code, p.added, p.modified, p.attributes::text, p.related::text from product p where p.deleted is null and p.partner_id = ? and p.product_type_id = ? and ? <@ (p.attributes->'")
        .append(Attribute.TV_CHANNEL_ID)
        .append("')")
        .toString();

    /**
     * Returns products of type package that haven't been deleted yet,
     * that contain subscription to the specified TV-channel.
     * @param partner
     * @param tvChannelCode
     * @return
     */
    @Override
    public List<PackageProduct> getPackagesForTvChannel(final Transaction transaction, final Partner partner, final String tvChannelCode) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        try (final PreparedStatement st = link.prepareStatement(getPackagesForTvChannelSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForPackage);
            final PGobject jsonTvChannel = new PGobject();
            jsonTvChannel.setType("jsonb");
            jsonTvChannel.setValue("\"" + tvChannelCode.replace("\"", "\\\"") + "\"");
            st.setObject(3, jsonTvChannel);
            try (final ResultSet rs = st.executeQuery()) {
                return readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForPackage, rs, PackageProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to obtain a list of packages for a TV-channel: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public Set<Matcher> getMatchers(final Transaction transaction, final MatcherKey key) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final StringBuilder constructionLog = new StringBuilder(400);
        constructionLog.append("[Thread ").append(Thread.currentThread().getId()).append("] getMatchers(): ");
        final String prefix = constructionLog.toString();
        final int initialConstructionLogLength = constructionLog.length();
        final Set<Matcher> foundMatchers = new HashSet<>();
        final MatchCondition condition = key.getCondition();
        final StringBuilder sql = new StringBuilder(200 + (condition.attributes.values.size() + condition.related.values.size()) * 50);
        final List<String> args = new ArrayList<>(condition.attributes.values.size() + condition.related.values.size());
        sql.append("select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where product_type_id = ?");
        assembleJsonSqlCondition(sql, "attributes", condition.attributes, args);
        assembleJsonSqlCondition(sql, "related", condition.related, args);
        sql.append(" for update");
        final String sqlString = sql.toString();
        try (final PreparedStatement st = link.prepareStatement(sqlString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, condition.productTypeId);
            int i = 2;
            for (final String arg : args) {
                final PGobject jsonArg = new PGobject();
                jsonArg.setType("jsonb");
                jsonArg.setValue(arg);
                st.setObject(i++, jsonArg);
            }
            try (final ResultSet rs = st.executeQuery()) {
                final Product p = productFromDb(transaction.getUtcCalendar(), provider, rs, constructionLog);
                if (p != null) {
                    try {
                        foundMatchers.add((Matcher)p);
                    }
                    catch (ClassCastException e) {
                        log.error(prefix + "Could not cast to a Matcher instance product with ID " + p.id + " of type " + p.getClass().getCanonicalName() + ": " + e.toString(), e);
                    }
                }
            }
        }
        catch (SQLException e) {
            log.error("[Thread " + Thread.currentThread().getId() + "] " + "Failed to obtain a matcher using the SQL on the next line: " + e.toString() + "\n" + sqlString, e);
            throw new DatabaseException("Failed to obtain a matcher: " + e.toString(), e);
        }
        catch (ResponseException e) {
            final String reason = "Failed to construct a matcher matched by the given matcher: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        if (constructionLog.length() > initialConstructionLogLength) {
            log.warn(constructionLog.toString());
        }

        return foundMatchers;
    }

    private void assembleJsonSqlCondition(final StringBuilder sql, final String jsonFieldName, final AttributeValues attributeValues, final List<String> args) {
        for (final Map.Entry<Attribute, Value> entry : attributeValues.values.entrySet()) {
            final Attribute attribute = entry.getKey();
            final Value value = entry.getValue();
            if (value.equals(null)) {
                sql.append(" and (not jsonb_exists(").append(jsonFieldName).append(", '").append(attribute.getIdentifier()).append("'))");
            }
            else {
                sql.append(" and ").append(jsonFieldName).append(" @> ?");
                final StringBuilder sb = new StringBuilder(100);
                sb.append("{\"");
                Value.escapeJson(attribute.getIdentifier(), sb);
                sb.append("\":");
                value.toJson(sb);
                sb.append("}");
                args.add(sb.toString());
            }
        }
    }

/*
update product
set related = (
    select '{' || array_to_string(array_append(array_agg(to_json(j.key) || ':' || j.value::text), '"video-id":1'), ',') || '}'
    from jsonb_each(related) j
    where j.key != 'video-id'
)::jsonb
where related->>'video-id' = '2'
*/
    private static final String setRelatedFieldSql = new StringBuilder(1024)
        .append("update product set related = (")
        .append("select '{' || array_to_string(array_append(array_agg(to_json(j.key) || ':' || j.value::text), cast(? as text)), ',') || '}' ") // param 1, string: new key:value
        .append("from jsonb_each(related) j ")
        .append("where j.key != ?") // param 2, string: the name of the key
        .append(")::jsonb where related->>? = ?") // param 3, string: key name; param 4, string: old value as a string
        .toString();

    /**
     * Use this method asynchronously and atomically, not e.g. within processing of a product, or else you may get a deadlock!
     *
     * @param transaction
     * @param attribute
     * @param oldValue
     * @param newValue
     */
    @Deprecated
    @Override
    public void setRelatedFieldForAll(final Transaction transaction, final Attribute attribute, final Value oldValue, final Value newValue) {
        final StringBuilder oldValueBuilder = new StringBuilder(128);
        oldValue.toJson(oldValueBuilder);
        final StringBuilder newKeyValueBuilder = new StringBuilder(256);
        newKeyValueBuilder.append("\"");
        Value.escapeJson(attribute.getIdentifier(), newKeyValueBuilder);
        newKeyValueBuilder.append("\":");
        newValue.toJson(newKeyValueBuilder);
        final int updateCount;
        try (final PreparedStatement st = ((JdbcDataLink)transaction.getLink()).prepareStatement(setRelatedFieldSql)) {
            st.setString(1, newKeyValueBuilder.toString());
            st.setString(2, attribute.getIdentifier());
            st.setString(3, attribute.getIdentifier());
            st.setString(4, oldValueBuilder.toString());
            updateCount = st.executeUpdate();
        }
        catch (SQLException e) {
            final String reason = "Failed to update related field for attribute " + attribute.getIdentifier() + " from value " + oldValue.asString() + " to value " + newValue.asString() + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("[Thread " + Thread.currentThread().getId() + "] Updated " + updateCount + " products/matchers changing their related field's attribute " + attribute.getIdentifier() + " from " + oldValue.asString() + " to " + newValue.asString());
    }

    private static final class PackageListWithTvChannelSubscriptionStatus {
        final List<PackageProduct> productPackages = new ArrayList<>();
        boolean isSubscribed;
        PackageListWithTvChannelSubscriptionStatus(final PackageProduct initialPackage, final boolean initialIsSubscribed) {
            productPackages.add(initialPackage);
            isSubscribed = initialIsSubscribed;
        }
    }

    @Override
    public void tvProgrammesInIntervalForPartner(
            final Transaction transaction,
            final Partner partner,
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();

        final Map<String, PackageListWithTvChannelSubscriptionStatus> packagesMap = new HashMap<>(); // tv-channel code -> list of PackageProduct instances
        for (final PackageProduct partnerPackage : listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
            final boolean isSubscribed = subscriptionPackageIds.contains(partnerPackage.id);
            for (final String code : partnerPackage.tvChannelCodes) {
                final PackageListWithTvChannelSubscriptionStatus existingPackageList = packagesMap.get(code);
                if (existingPackageList == null) {
                    packagesMap.put(code, new PackageListWithTvChannelSubscriptionStatus(partnerPackage, isSubscribed));
                }
                else {
                    existingPackageList.productPackages.add(partnerPackage);
                    if (isSubscribed) existingPackageList.isSubscribed = true;
                }
            }
        }

        final Map<String, TvChannelData> tvChannelDatas = new HashMap<>();
        for (final TvChannelProduct product : listByType(transaction, partner, productTypeCodes.tvChannel, TvChannelProduct.class)) {
            final String tvChannelCode = product.partnerProductCode;
            final PackageListWithTvChannelSubscriptionStatus packageList = packagesMap.get(tvChannelCode);
            if (packageList == null) continue; // tv-channel not in any package
            tvChannelDatas.put(tvChannelCode, new TvChannelData(product, packageList.productPackages, packageList.isSubscribed));
        }

/*
select pr.id, pr.partner_product_code, pr.product_id
from product pr
inner join product pu on pu.id = pr.product_id
inner join product_attribute b on b.product_id = pu.id and b.attribute_id = 56
inner join product_attribute e on e.product_id = pu.id and e.attribute_id = 57
where pr.partner_id = 6 and pr.product_type_id = 3 and pr.deleted is null
and (
  (cast(b.original_value as bigint) >= 1397745240 and cast(b.original_value as bigint) <= 1397752440) or -- min, max
  (cast(b.original_value as bigint) < 1397748840 and cast(e.original_value as bigint) > 1397748840)) -- now

select id, partner_product_code, attributes::text, related::text
from product
where partner_id = 6 and product_type_id = 3 and deleted is null
and (
  (cast(attributes->>'begin-time' as bigint) >= 1397745240 and cast(attributes->>'begin-time' as bigint) <= 1397752440) or -- min, max
  (cast(attributes->>'begin-time' as bigint) < 1397748840 and cast(attributes->>'end-time' as bigint) > 1397748840)) -- now

*/
        final long minTime = recommendTimeMillis - negativeOffsetMillis;
        final long maxTime = recommendTimeMillis + positiveOffsetMillis;
        final List<TvProgrammeProduct> tvProgrammeProducts;
        try (final PreparedStatement st = link.prepareStatement(tvProgrammeInIntervalSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvProgramme);
            st.setLong(3, minTime / 1000L);
            st.setLong(4, maxTime / 1000L);
            st.setLong(5, recommendTimeMillis / 1000L);
            st.setLong(6, recommendTimeMillis / 1000L);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                tvProgrammeProducts = readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForTvProgramme, rs, TvProgrammeProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list tv-programmes: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("Obtained " + tvProgrammeProducts.size() + " tv-programmes in the range [" + recommendTimeMillis + "-" + negativeOffsetMillis + ", " + recommendTimeMillis + "+" + positiveOffsetMillis + "]");

        int videoIdMisses = 0;
        int tvChannelMisses = 0;
        int catchupMisses = 0;
        int tvProgrammeMisses = 0;
        final int startingSize = outputBuilder.size();
        for (final TvProgrammeProduct tvProgramme : tvProgrammeProducts) {
            if (tvProgramme.videoMatchId < 0L) { // no such video
                videoIdMisses++;
                continue;
            }

            List<TvChannelData> reachableThroughTvChannels = null;
            final int tvChannelCount = tvProgramme.tvChannelCodes.length;
            if (tvChannelCount == 0) { // no TV-channel
                tvProgrammeMisses++;
                continue;
            }
            boolean isSubscribed = false;
            for (int i = 0; i < tvChannelCount; i++) {
                final TvChannelData tvChannelData = tvChannelDatas.get(tvProgramme.tvChannelCodes[i]);
                if (tvChannelData == null) { // no such TV-channel
                    tvChannelMisses++;
                    continue;
                }
                if ((tvProgramme.beginTimeMillis < (recommendTimeMillis - tvChannelData.tvChannel.catchupMillis)) && (tvProgramme.endTimeMillis < recommendTimeMillis)) { // out of catch-up range, and not currently running
                    catchupMisses++;
                    continue;
                }
                if (reachableThroughTvChannels == null) reachableThroughTvChannels = new ArrayList<>();
                reachableThroughTvChannels.add(tvChannelData);
                if (tvChannelData.isSubscribed) isSubscribed = true;
            }
            if (reachableThroughTvChannels == null) { // no such TV-channel or tv-programme out of catchup range
                continue;
            }

            outputBuilder.add(VideoData.forTvProgramme(
                    tvProgramme,
                    reachableThroughTvChannels,
                    isSubscribed
            ));
        }
        log.debug("After verifications there are " + (outputBuilder.size() - startingSize) + " unique tv-programmes remaining; " + videoIdMisses + " had missing video-id, " + tvChannelMisses + " referenced a non-existent tv-channel code, " + catchupMisses + " were not inside catch-up for their channel, " + tvProgrammeMisses + " has no tv-channels defined");
    }

    private static final class PackageListWithVodSubscriptionStatus {
        final List<PackageProduct> productPackages;
        boolean isSubscribed;
        final VodProduct vod;
        PackageListWithVodSubscriptionStatus(final VodProduct vod) {
            this.productPackages = new ArrayList<>();
            this.isSubscribed = false;
            this.vod = vod;
        }
    }

    @Override
    public void vodForPartner(
            final Transaction transaction,
            final Partner partner,
            final long recommendTimeMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        final Map<String, PackageListWithVodSubscriptionStatus> catalogueMapping = new HashMap<>();
        for (final VodProduct vod : listByType(transaction, partner, productTypeCodes.svod, VodProduct.class)) {
            catalogueMapping.put(vod.catalogueId, new PackageListWithVodSubscriptionStatus(vod));
        }
        for (final VodProduct vod : listByType(transaction, partner, productTypeCodes.tvod, VodProduct.class)) {
            catalogueMapping.put(vod.catalogueId, new PackageListWithVodSubscriptionStatus(vod));
        }

        for (final PackageProduct partnerPackage : listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
            final boolean isSubscribed = subscriptionPackageIds.contains(partnerPackage.id);
            for (final String code : partnerPackage.vodCodes) {
                final PackageListWithVodSubscriptionStatus p = catalogueMapping.get(code);
                if (p == null) {
                    continue;
                }
                p.productPackages.add(partnerPackage);
                if (isSubscribed) p.isSubscribed = true;
            }
        }


/*
select pr.id, pr.partner_product_code, pr.product_id
from product pr
inner join log_product l on l.log_id = pr.last_log_id
where pr.partner_id = 6 and pr.product_type_id = 1 and pr.deleted is null
and l.valid_from <= '2014-01-15'
*/
        final List<VideoProduct> videoProducts;
        try (final PreparedStatement st = link.prepareStatement(vodForPartnerSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForVideo);
            st.setLong(3, recommendTimeMillis / 1000L);
            st.setLong(4, recommendTimeMillis / 1000L);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                videoProducts = readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForVideo, rs, VideoProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list VOD videos: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("Obtained " + videoProducts.size() + " VOD products for recommendations at time " + recommendTimeMillis);

        int catalogueMisses = 0;
        int countProcessed = 0;
        final int startingSize = outputBuilder.size();
        for (final VideoProduct video : videoProducts) {
            final PackageListWithVodSubscriptionStatus catalogueInfo = catalogueMapping.get(video.catalogueId);
            if (catalogueInfo == null) {
                catalogueMisses++;
                continue;
            }
            outputBuilder.add(VideoData.forVideo(video, catalogueInfo.vod, catalogueInfo.productPackages, catalogueInfo.isSubscribed));
            countProcessed++;
        }
        log.debug("After verifications there are " + (outputBuilder.size() - startingSize) + " unique VODs remaining; " + /*expiredMisses + " expired, " +*/ catalogueMisses + " referenced a non-existent VOD catalogue, " + countProcessed + " VODs processed");
    }

    private static final class LazyVideoInfoEvaluator {
        // This is actually not lazy, because of the nature of the SQL database it is more efficient to just punch a few fixed selects at the beginning than issue a separate set of selects per product.
        // But it is lazy in the sense that it is instantiated only if needed.
        final Map<String, PackageListWithVodSubscriptionStatus> catalogueMapping = new HashMap<>();

        LazyVideoInfoEvaluator(final TLongSet subscriptionPackageIds, final ProductManagerImpl owner, final Partner partner, final Transaction transaction) {
            final ProductTypeCodes productTypeCodes = transaction.getLink().getProvider().getProductTypeCodes();
            final Map<String, PackageListWithVodSubscriptionStatus> catalogueMapping = this.catalogueMapping; // local variable for faster access
            for (final VodProduct vod : owner.listByType(transaction, partner, productTypeCodes.svod, VodProduct.class)) {
                catalogueMapping.put(vod.catalogueId, new PackageListWithVodSubscriptionStatus(vod));
            }
            for (final VodProduct vod : owner.listByType(transaction, partner, productTypeCodes.tvod, VodProduct.class)) {
                catalogueMapping.put(vod.catalogueId, new PackageListWithVodSubscriptionStatus(vod));
            }

            for (final PackageProduct partnerPackage : owner.listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
                final boolean isSubscribed = subscriptionPackageIds.contains(partnerPackage.id);
                for (final String code : partnerPackage.vodCodes) {
                    final PackageListWithVodSubscriptionStatus p = catalogueMapping.get(code);
                    if (p == null) {
                        continue;
                    }
                    p.productPackages.add(partnerPackage);
                    if (isSubscribed) p.isSubscribed = true;
                }
            }
        }

        PackageListWithVodSubscriptionStatus getCatalogueInfo(final String catalogueId) {
            return catalogueMapping.get(catalogueId);
        }
    }

    private static final class TvChannelListWithSubscriptionStatus {
        final List<TvChannelData> availableTvChannels;
        final boolean isSubscribed;
        TvChannelListWithSubscriptionStatus(final List<TvChannelData> availableTvChannels, final boolean isSubscribed) {
            this.availableTvChannels = availableTvChannels;
            this.isSubscribed = isSubscribed;
        }
    }

    private static final class LazyTvProgrammeInfoEvaluator {
        // This is actually not lazy, because of the nature of the SQL database it is more efficient to just punch a few fixed selects at the beginning than issue a separate set of selects per product.
        // But it is lazy in the sense that it is instantiated only if needed.
        final TLongSet subscriptionPackageIds;
        final Map<String, TvChannelData> dataMapping = new HashMap<>();
        final Map<String, TvChannelListWithSubscriptionStatus> tvChannelMapping = new HashMap<>();

        LazyTvProgrammeInfoEvaluator(final TLongSet subscriptionPackageIds, final ProductManagerImpl owner, final Partner partner, final Transaction transaction) {
            this.subscriptionPackageIds = subscriptionPackageIds;
            final ProductTypeCodes productTypeCodes = transaction.getLink().getProvider().getProductTypeCodes();
            final Map<String, PackageListWithTvChannelSubscriptionStatus> packagesMap = new HashMap<>(); // tv-channel code -> list of PackageProduct instances
            for (final PackageProduct partnerPackage : owner.listByType(transaction, partner, productTypeCodes.package_, PackageProduct.class)) {
                final boolean isSubscribed = subscriptionPackageIds.contains(partnerPackage.id);
                for (final String code : partnerPackage.tvChannelCodes) {
                    final PackageListWithTvChannelSubscriptionStatus existingPackageList = packagesMap.get(code);
                    if (existingPackageList == null) {
                        packagesMap.put(code, new PackageListWithTvChannelSubscriptionStatus(partnerPackage, isSubscribed));
                    }
                    else {
                        existingPackageList.productPackages.add(partnerPackage);
                        if (isSubscribed) existingPackageList.isSubscribed = true;
                    }
                }
            }

            final Map<String, TvChannelData> tvChannelDatas = this.dataMapping; // local variable for faster access
            for (final TvChannelProduct product : owner.listByType(transaction, partner, productTypeCodes.tvChannel, TvChannelProduct.class)) {
                final String tvChannelCode = product.partnerProductCode;
                final PackageListWithTvChannelSubscriptionStatus packageList = packagesMap.get(tvChannelCode);
                if (packageList == null) continue; // tv-channel not in any package
                tvChannelDatas.put(tvChannelCode, new TvChannelData(product, packageList.productPackages, packageList.isSubscribed));
            }
        }

        TvChannelListWithSubscriptionStatus getTvChannelsInfo(final String[] tvChannelCodes) {
            final String concatenatedCodes = concat(tvChannelCodes);
            final TvChannelListWithSubscriptionStatus status = tvChannelMapping.get(concatenatedCodes);
            if (status != null) return status;
            final List<TvChannelData> tvChannelDatas = new ArrayList<>();
            boolean isSubscribed = false;
            for (final String tvChannelCode : tvChannelCodes) {
                final TvChannelData existingData = dataMapping.get(tvChannelCode);
                if (existingData == null) {
                    log.error("There is no TV-channel with partner_product_code=" + tvChannelCode);
                }
                else {
                    tvChannelDatas.add(existingData);
                    if (existingData.isSubscribed) isSubscribed = true;
                }
            }
            final TvChannelListWithSubscriptionStatus newStatus = new TvChannelListWithSubscriptionStatus(tvChannelDatas, isSubscribed);
            tvChannelMapping.put(concatenatedCodes, newStatus);
            return newStatus;
        }

        private final StringBuilder concatenator = new StringBuilder(50);

        String concat(final String[] codes) {
            if ((codes == null) || (codes.length == 0)) return "";
            if (codes.length == 1) return codes[0];
            final StringBuilder c = concatenator; // local variable, for faster access
            c.setLength(0);
            c.append(codes[0]);
            final int n = codes.length;
            for (int i = 1; i < n; i++) c.append(" ").append(codes[i]);
            return c.toString();
        }
    }

    @Override
    public void buildDatasetFromVideos(
            final Transaction transaction,
            final Iterable<? extends GeneralVideoProduct> videos,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        final TLongObjectMap<LazyVideoInfoEvaluator> partnerVideoEvaluators = new TLongObjectHashMap<>();
        final TLongObjectMap<LazyTvProgrammeInfoEvaluator> partnerTvProgrammeEvaluators = new TLongObjectHashMap<>();

        for (final GeneralVideoProduct p : videos) {
            if (p instanceof VideoProduct) {
                final VideoProduct video = (VideoProduct)p;
                final String catalogueId = video.catalogueId;
                if (catalogueId == null) {
                    log.error("buildDatasetFromVideos(): Video product with product_id=" + video.id + " has no catalogue ID");
                    continue;
                }
                LazyVideoInfoEvaluator evaluator = partnerVideoEvaluators.get(video.partnerId);
                if (evaluator == null) {
                    evaluator = new LazyVideoInfoEvaluator(subscriptionPackageIds, this, transaction.getLink().getPartnerManager().getById(video.partnerId), transaction);
                    partnerVideoEvaluators.put(video.partnerId, evaluator);
                }
                final PackageListWithVodSubscriptionStatus catalogueInfo = evaluator.getCatalogueInfo(catalogueId);
                if (catalogueInfo == null) {
                    log.error("buildDatasetFromVideos(): Could not determine catalogue info for video product with product_id=" + video.id + " having catalogue ID " + catalogueId);
                    continue;
                }
                outputBuilder.add(VideoData.forVideo(video, catalogueInfo.vod, catalogueInfo.productPackages, catalogueInfo.isSubscribed));
            }
            else if (p instanceof TvProgrammeProduct) {
                final TvProgrammeProduct tvProgramme = (TvProgrammeProduct)p;
                final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
                if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                    log.error("buildDatasetFromVideos(): TV-programme product with product_id=" + tvProgramme.id + " has no TV-channel codes assigned");
                    continue;
                }
                LazyTvProgrammeInfoEvaluator evaluator = partnerTvProgrammeEvaluators.get(tvProgramme.partnerId);
                if (evaluator == null) {
                    evaluator = new LazyTvProgrammeInfoEvaluator(subscriptionPackageIds, this, transaction.getLink().getPartnerManager().getById(tvProgramme.partnerId), transaction);
                    partnerTvProgrammeEvaluators.put(tvProgramme.partnerId, evaluator);
                }
                final TvChannelListWithSubscriptionStatus status = evaluator.getTvChannelsInfo(tvChannelCodes);
                if (status == null) {
                    log.error("buildDatasetFromVideos(): Could not determine TV-channels info for tv-programme product with product_id=" + tvProgramme.id);
                    continue;
                }
                outputBuilder.add(VideoData.forTvProgramme(tvProgramme, status.availableTvChannels, status.isSubscribed));
            }
            else {
                throw new DatabaseException("Unknown video product in the list of supplied videos: " + p.getClass().getCanonicalName() + ", product_id=" + p.id);
            }
        }
    }

    @Override
    public List<TvChannelProduct> getTvChannelsForPartner(final Transaction transaction, final Partner partner) {
        return listByType(transaction, partner, ((JdbcDataLink)transaction.getLink()).getProvider().getProductTypeCodes().tvChannel, TvChannelProduct.class);
    }

    /*
    select id, partner_product_code, attributes::text, related::text
    from product
    where partner_id = 6 and product_type_id = 3 and deleted is null
    and '"MTV"'::jsonb <@ (p.attributes->'tv-channel');
    and (
      (cast(attributes->>'begin-time' as bigint) >= 1397745240 and cast(attributes->>'begin-time' as bigint) <= 1397752440) or -- index cutoff to localize to a partition, higher bound
      cast(attributes->>'end-time' as bigint) > 1397748840) -- lower bound
    */
    private static final String tvProgrammesOverlappingSql = new StringBuilder(500)
            .append("select id, partner_product_code, added, modified, attributes::text, related::text from product where partner_id = ? and product_type_id = ? and deleted is null and ? <@ (attributes->'")
            .append(Attribute.TV_CHANNEL)
            .append("') and ((cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) >= ? and cast(attributes->>'")
            .append(Attribute.BEGIN_TIME)
            .append("' as bigint) <= ?) or cast(attributes->>'")
            .append(Attribute.END_TIME)
            .append("' as bigint) > ?)")
            .toString();

    @Override
    public void forEachOverlappingTvProgramme(final Transaction transaction, final Partner partner, final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final ProductTypeCodes productTypeCodes = provider.getProductTypeCodes();
        final List<TvProgrammeProduct> tvProgrammeProducts;
        try (final PreparedStatement st = link.prepareStatement(tvProgrammesOverlappingSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partner.getId());
            st.setLong(2, productTypeCodes.idForTvProgramme);
            final PGobject jsonTvChannel = new PGobject();
            jsonTvChannel.setType("jsonb");
            jsonTvChannel.setValue("\"" + tvChannelCode + "\"");
            st.setObject(3, jsonTvChannel);
            st.setLong(4, (beginTimeMillis - 43200000L) / 1000L); // begin-time index cutoff is half a day before beginTimeMillis
            st.setLong(5, endTimeMillis / 1000L);
            st.setLong(6, beginTimeMillis / 1000L);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                tvProgrammeProducts = readProducts(transaction.getUtcCalendar(), provider, partner.getId(), productTypeCodes.idForTvProgramme, rs, TvProgrammeProduct.class);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list tv-programmes: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        log.debug("Obtained " + tvProgrammeProducts.size() + " tv-programmes overlapping the range [" + beginTimeMillis + ", " + endTimeMillis + "] on TV-channel " + tvChannelCode + " for partner " + partner.getId());
        tvProgrammeProducts.forEach(consumer);
    }

    // param 1, string: key name; param 2, string: value as a string
    private static final String getRelatedFieldSql = "select id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text from product where related->>? = ? for update";
    // currently this is not used, but we retain it for a possible future use
    public List<Product> getAllRelatedProducts(final Transaction transaction, final Attribute attribute, final Value value) {
        final StringBuilder valueBuilder = new StringBuilder(128);
        value.toJson(valueBuilder);
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final List<Product> foundProducts = new ArrayList<>();
        final StringBuilder constructionLog = new StringBuilder(400);
        constructionLog.append("getAllRelatedProducts(): ");
        final int initialConstructionLogLength = constructionLog.length();
        try (final PreparedStatement st = link.prepareStatement(getRelatedFieldSql)) {
                st.setString(1, attribute.getIdentifier());
                st.setString(2, valueBuilder.toString());
                try (final ResultSet rs = st.executeQuery()) {
                    foundProducts.addAll(productsFromDb(transaction.getUtcCalendar(), provider, rs, constructionLog));
                }
        }
        catch(SQLException e){
            final String reason = "Failed to run the query to get overlapping TV shows: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
        catch(ResponseException e){
            final String reason = "Failed to construct products of overlapping TV shows: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
        finally{
            if (constructionLog.length() > initialConstructionLogLength) {
                log.warn(constructionLog.toString());
            }
        }
        return foundProducts;
    }

    @Override
    public void deleteByPartnerAndTypeAndCode(final Transaction transaction, final Partner partner, final ProductType productType, final String productCode) {
        try (final PreparedStatement st = ((JdbcDataLink)transaction.getLink()).prepareStatement("update product set deleted = current_timestamp at time zone 'utc' where partner_id = ? and product_type_id = ? and partner_product_code = ? and deleted is null")) {
            if (partner == null) st.setNull(1, Types.BIGINT);
            else st.setLong(1, partner.getId());
            if (productType == null) st.setNull(2, Types.BIGINT);
            else st.setLong(2, productType.getId());
            if (productCode == null) st.setNull(3, Types.VARCHAR);
            else st.setString(3, productCode);
            st.executeUpdate();
        }
        catch (SQLException e) {
            final String reason = "Failed to mark as deleted a product specified with partner, product type and product code: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    private static final String deleteProductById = "update product set deleted = ? where id = ? returning id, partner_id, product_type_id, partner_product_code, deleted, modified, added, related::text, attributes::text";

    @Override
    public Product delete(final Transaction transaction, final long id) {
        final StringBuilder constructionLog = new StringBuilder(400);
        constructionLog.append("[Thread ").append(Thread.currentThread().getId()).append("] delete(): ");
        final int initialConstructionLogLength = constructionLog.length();
        try (final PreparedStatement st = ((JdbcDataLink)transaction.getLink()).prepareStatement(deleteProductById)) {
            st.setTimestamp(1, new Timestamp(Timer.currentTimeMillis()), transaction.getUtcCalendar());
            st.setLong(2, id);
            try (final ResultSet rs = st.executeQuery()) {
                return productFromDb(transaction.getUtcCalendar(), transaction.getLink().getProvider(), rs, constructionLog);
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to run the query to delete a product by given ID: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        catch (ResponseException e) {
            final String reason = "Failed to construct the product deleted by given ID: " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        finally{
            if (constructionLog.length() > initialConstructionLogLength) {
                log.warn(constructionLog.toString());
            }
        }
    }

    @Override
    public Product save(final Transaction transaction, final Product product) {
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        if (product.id <= 0L) {
            // create the product
            try (final PreparedStatement st = link.prepareStatement("insert into product (partner_id, product_type_id, partner_product_code, deleted, attributes, related) values (?, ?, ?, ?, ?, ?) returning id, partner_product_code, added, modified, attributes::text, related::text, partner_id, product_type_id, deleted")) {
                if (product.partnerId < 0L) st.setNull(1, Types.BIGINT);
                else st.setLong(1, product.partnerId);
                if (product.productTypeId == 0L) st.setNull(2, Types.BIGINT);
                else st.setLong(2, product.productTypeId);
                if (product.partnerProductCode == null) st.setNull(3, Types.VARCHAR);
                else st.setString(3, product.partnerProductCode);
                if (product.deleted == null) st.setNull(4, Types.TIMESTAMP);
                else st.setTimestamp(4, product.deleted, transaction.getUtcCalendar());

                final PGobject attributes = new PGobject();
                attributes.setType("jsonb");
                if (product.attributes == null) attributes.setValue("{}");
                else attributes.setValue(product.attributes.toJson());
                st.setObject(5, attributes);

                final PGobject related = new PGobject();
                related.setType("jsonb");
                if (product.related == null) related.setValue("{}");
                else related.setValue(product.related.toJson());
                st.setObject(6, related);

                try (final ResultSet rs = st.executeQuery()) {
                    rs.next();
                    Timestamp deleted = rs.getTimestamp(9, transaction.getUtcCalendar());
                    if (rs.wasNull()) deleted = null;
                    return readProduct(transaction.getUtcCalendar(), provider, rs.getLong(7), rs.getLong(8), deleted, rs, null, Product.class);
                }
            }
            catch (SQLException e) {
                final String reason = "Failed to create a product: " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
            catch (ResponseException e) {
                final String reason = "Failed to create a product: failed to instantiate a Product: " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
            catch (ClassCastException e) {
                final String reason = "Failed to create a product: failed to cast a Product instance to " + Product.class.getCanonicalName() + ": " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
        }
        else {
            // modify the product
            try (final PreparedStatement st = link.prepareStatement("update product set partner_id = ?, product_type_id = ?, partner_product_code = ?, deleted = ?, attributes = ?, related = ? where id = ? returning id, partner_product_code, added, modified, attributes::text, related::text, partner_id, product_type_id, deleted")) {
                if (product.partnerId < 0L) st.setNull(1, Types.BIGINT);
                else st.setLong(1, product.partnerId);
                if (product.productTypeId == 0L) st.setNull(2, Types.BIGINT);
                else st.setLong(2, product.productTypeId);
                if (product.partnerProductCode == null) st.setNull(3, Types.VARCHAR);
                else st.setString(3, product.partnerProductCode);
                if (product.deleted == null) st.setNull(4, Types.TIMESTAMP);
                else st.setTimestamp(4, product.deleted, transaction.getUtcCalendar());

                final PGobject attributes = new PGobject();
                attributes.setType("jsonb");
                if (product.attributes == null) attributes.setValue("{}");
                else attributes.setValue(product.attributes.toJson());
                st.setObject(5, attributes);

                final PGobject related = new PGobject();
                related.setType("jsonb");
                if (product.related == null) related.setValue("{}");
                else related.setValue(product.related.toJson());
                st.setObject(6, related);

                st.setLong(7, product.id);
                try (final ResultSet rs = st.executeQuery()) {
                    rs.next();
                    Timestamp deleted = rs.getTimestamp(9, transaction.getUtcCalendar());
                    if (rs.wasNull()) deleted = null;
                    return readProduct(transaction.getUtcCalendar(), provider, rs.getLong(7), rs.getLong(8), deleted, rs, null, Product.class);
                }
            }
            catch (SQLException e) {
                final String reason = "Failed to modify a product: " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
            catch (ResponseException e) {
                final String reason = "Failed to modify a product: failed to instantiate a Product with ID " + product.id + ": " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
            catch (ClassCastException e) {
                final String reason = "Failed to modify a product: failed to cast a Product instance with ID " + product.id + " to " + Product.class.getCanonicalName() + ": " + e.toString();
                log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
                throw new DatabaseException(reason, e);
            }
        }
    }

    /**
     * Returns a list of all partner's products of the given type. Only active
     * (not deleted) products are returned.
     *
     * @param partner
     * @param productType
     * @return
     */
    private <L extends Product> List<L> listByType(final Transaction transaction, final Partner partner, final ProductType productType, final Class<L> clazz) {
        final List<L> result = new ArrayList<>();
        final long productTypeId = productType.getId().longValue();
        final long partnerId = partner.getId().longValue();
        final StringBuilder logBuilder = new StringBuilder(500);
        final long threadId = Thread.currentThread().getId();
        final JdbcDataLink link = (JdbcDataLink)transaction.getLink();
        final DataProvider provider = link.getProvider();
        final Calendar cal = transaction.getUtcCalendar();
        try (final PreparedStatement st = link.prepareStatement("select p.id, p.partner_product_code, p.added, p.modified, p.attributes::text, p.related::text from product p where p.partner_id = ? and p.product_type_id = ? and p.deleted is null", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partnerId);
            st.setLong(2, productTypeId);
            st.setFetchSize(1000);
            try (final ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    final long productId = rs.getLong(1);
                    String code = rs.getString(2);
                    if (rs.wasNull()) code = null;
                    Timestamp added = rs.getTimestamp(3, cal);
                    if (rs.wasNull()) added = null;
                    Timestamp modified = rs.getTimestamp(4, cal);
                    if (rs.wasNull()) modified = null;
                    String attributes = rs.getString(5);
                    if (rs.wasNull()) attributes = null;
                    String related = rs.getString(6);
                    if (rs.wasNull()) related = null;
                    final Product p;
                    try {
                        p = Product.create(productId, productTypeId, partnerId, code, added, modified, null, attributes, related, provider, logBuilder);
                    }
                    catch (ResponseException e) {
                        log.error("[Thread " + threadId + "] Failed to instantiate a Product with ID " + productId + " (ignored): " + e.toString(), e);
                        continue;
                    }
                    if (logBuilder.length() > 0) {
                        log.error("[Thread " + threadId + "] Errors while instantiating a Product with ID " + productId + " (ignored):\n" + logBuilder.toString());
                        logBuilder.delete(0, logBuilder.length());
                    }
                    try {
                        result.add(clazz.cast(p));
                    }
                    catch (ClassCastException e) {
                        log.error("Thread " + threadId + "] Failed to cast a Product instance with ID " + productId + " to " + clazz.getCanonicalName() + ": " + e.toString(), e);
                    }
                }
            }
        }
        catch (SQLException e) {
            final String reason = "Failed to list products of type " + productType.getIdentifier() + ": " + e.toString();
            log.error("[Thread " + Thread.currentThread().getId() + "] " + reason, e);
            throw new DatabaseException(reason, e);
        }
        return result;
    }

    // must provide a ResultSet returning these fields in the specified order:
    // id, partner_product_code, attributes::text, related::text
    // Assumption: "deleted" is null for all rows of the ResultSet
    private <L extends Product> List<L> readProducts(final Calendar utcCalendar, final DataProvider provider, final long partnerId, final long productTypeId, final ResultSet resultSet, final Class<L> clazz) throws SQLException {
        final List<L> result = new ArrayList<>();
        final long threadId = Thread.currentThread().getId();
        final StringBuilder logBuilder = new StringBuilder(500);
        while (resultSet.next()) {
            try {
                result.add(readProduct(utcCalendar, provider, partnerId, productTypeId, null, resultSet, logBuilder, clazz));
            }
            catch (ResponseException e) {
                log.error("[Thread " + threadId + "] Failed to instantiate a Product with ID " + resultSet.getLong(1) + " (ignored): " + e.toString(), e);
            }
            catch (ClassCastException e) {
                log.error("[Thread " + threadId + "] Failed to cast a Product instance with ID " + resultSet.getLong(1) + " to " + clazz.getCanonicalName() + " (ignored): " + e.toString(), e);
            }
            if (logBuilder.length() > 0) {
                log.error("[Thread " + threadId + "] Errors while instantiating a Product with ID " + resultSet.getLong(1) + " (ignored):\n" + logBuilder.toString());
                logBuilder.delete(0, logBuilder.length());
            }
        }
        return result;
    }

    // must provide a ResultSet returning these fields in the specified order:
    // id, partner_product_code, attributes::text, related::text
    private <L extends Product> L readProduct(final Calendar utcCalendar, final DataProvider provider, final long partnerId, final long productTypeId, final Timestamp deleted, final ResultSet resultSet, final StringBuilder logBuilder, final Class<L> clazz) throws SQLException, ResponseException, ClassCastException {
        final long productId = resultSet.getLong(1);
        String code = resultSet.getString(2);
        if (resultSet.wasNull()) code = null;
        Timestamp added = resultSet.getTimestamp(3, utcCalendar);
        if (resultSet.wasNull()) added = null;
        Timestamp modified = resultSet.getTimestamp(4, utcCalendar);
        if (resultSet.wasNull()) modified = null;
        String attributes = resultSet.getString(5);
        if (resultSet.wasNull()) attributes = null;
        String related = resultSet.getString(6);
        if (resultSet.wasNull()) related = null;
        return clazz.cast(Product.create(productId, productTypeId, partnerId, code, added, modified, deleted, attributes, related, provider, logBuilder));
    }
}
