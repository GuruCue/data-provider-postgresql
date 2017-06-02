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
import com.gurucue.recommendations.entity.ConsumerEvent;
import com.gurucue.recommendations.entity.DataType;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entitymanager.ConsumerEventManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * Implementation of the consumer event manager interface for PostgreSQL.
 */
public final class ConsumerEventManagerImpl implements ConsumerEventManager {
    private static final Logger log = LogManager.getLogger(ConsumerEventManagerImpl.class);
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    private final PostgreSqlDataLink dataLink;

    ConsumerEventManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public void save(final ConsumerEvent consumerEvent) {
        if (consumerEvent == null) {
            log.error("Attempted to save a null consumer event");
            return;
        }
        final Product p = consumerEvent.getProduct();
        final Calendar cal = Calendar.getInstance();
        cal.setTimeZone(utcTimeZone);
        try (final CallableStatement st = dataLink.prepareCall("{ ? = call consumer_event_partition_insert_function(null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? , ?) }")) {
            st.registerOutParameter(1, Types.BIGINT);
            if (consumerEvent.getEventTimestamp() == null) st.setNull(2, Types.TIMESTAMP);
            else st.setTimestamp(2, consumerEvent.getEventTimestamp(), cal);
            if (consumerEvent.getPartner() == null) st.setNull(3, Types.BIGINT);
            else st.setLong(3, consumerEvent.getPartner().getId());
            if (consumerEvent.getConsumer() == null) st.setNull(4, Types.BIGINT);
            else st.setLong(4, consumerEvent.getConsumer().getId());
            if (p == null) st.setNull(5, Types.BIGINT);
            else st.setLong(5, p.id);//partner_product_id
            if (consumerEvent.getEventType() == null) st.setNull(6, Types.BIGINT);
            else st.setLong(6, consumerEvent.getEventType().getId());

            if (consumerEvent.getRequestTimestamp() == null) {
                log.error("ConsumerEvent has a null requestTimestamp, this will probably fail, because the partitioning logic depends on it");
                st.setNull(7, Types.TIMESTAMP);
            }
            else st.setTimestamp(7, consumerEvent.getRequestTimestamp(), cal);

            if (consumerEvent.getUserProfileId() == null) st.setNull(8, Types.BIGINT);
            else st.setLong(8, consumerEvent.getUserProfileId());

            if (consumerEvent.getResponseCode() == null) st.setNull(9, Types.INTEGER);
            else st.setInt(9, consumerEvent.getResponseCode());
            if (consumerEvent.getFailureCondition() == null) st.setNull(10, Types.VARCHAR);
            else st.setString(10, consumerEvent.getFailureCondition());
            if (consumerEvent.getFailedRequest() == null) st.setNull(11, Types.VARCHAR);
            else st.setString(11, consumerEvent.getFailedRequest());
            if (consumerEvent.getRequestDuration() == null) st.setNull(12, Types.BIGINT);
            else st.setLong(12, consumerEvent.getRequestDuration());

            final Map<DataType,String> dataMap = consumerEvent.getData();
            if ((dataMap != null) && !dataMap.isEmpty()) {
                StringBuilder jsonString = new StringBuilder(200);
                if (consumerEvent.getDataAsJsonString(jsonString)) { //getDataAsJsonString can return false if there is a problem with the data object
                    final PGobject jsonObj = new PGobject();
                    jsonObj.setType("jsonb");
                    jsonObj.setValue(jsonString.toString());
                    st.setObject(13, jsonObj);
                } else {
                    st.setObject(13, null);
                }
            } else st.setObject(13, null);

            st.execute();
            consumerEvent.setId(st.getLong(1));
        }
        catch (SQLException e) {
            final String reason = "Failed to insert a ConsumerEvent: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public void deleteById(final Long id) {
        try (final PreparedStatement st = dataLink.prepareStatement("delete from consumer_event where id = ?")) {
            st.setLong(1, id);
            st.executeUpdate();
        }
        catch (SQLException e) {
            final String reason = "Failed to delete a consumer event: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }
}
