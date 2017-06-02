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
import com.gurucue.recommendations.entity.LogSvcRecommendation;
import com.gurucue.recommendations.entitymanager.LogSvcSearchManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

public class LogSvcSearchManagerImpl implements LogSvcSearchManager {
    private static final Logger log = LogManager.getLogger(LogSvcSearchManagerImpl.class);
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    private final PostgreSqlDataLink dataLink;

    LogSvcSearchManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    private static PGobject createJsonbPGobject(final String json) throws SQLException {
        final PGobject result = new PGobject();
        result.setType("jsonb");
        result.setValue(json);
        return result;
    }

    @Override
    public void put(final LogSvcRecommendation searchLog) {
        if (searchLog == null) {
            log.error("Attempted to save a null search log");
            return;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTimeZone(utcTimeZone);
        try (final CallableStatement st = dataLink.prepareCall(
                "{ ? = call log_svc_search_partition_insert_function(null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }"
        )) {
            st.registerOutParameter(1, Types.BIGINT);
            st.setLong(2, searchLog.getPartner().getId());
            if (searchLog.getConsumerId() > 0L) st.setLong(3, searchLog.getConsumerId());
            else st.setNull(3, Types.BIGINT);
            if (searchLog.getMaxRecommendations() == null) st.setNull(4, Types.INTEGER);
            else st.setInt(4, searchLog.getMaxRecommendations());
            st.setInt(5, searchLog.getResponseCode());
            if (searchLog.getFailureCondition() == null) st.setNull(6, Types.VARCHAR);
            else st.setString(6, searchLog.getFailureCondition());
            if (searchLog.getFailedRequest() == null) st.setNull(7, Types.VARCHAR);
            else st.setString(7, searchLog.getFailedRequest());
            st.setTimestamp(8, searchLog.getRequestTimestamp(), cal);
            if (searchLog.getRequestDuration() == null) st.setNull(9, Types.BIGINT);
            else st.setLong(9, searchLog.getRequestDuration());
            if (searchLog.getPartnerRecommenderName() == null) st.setNull(10, Types.VARCHAR);
            else st.setString(10, searchLog.getPartnerRecommenderName());

            if (searchLog.getBlenderName() == null) st.setNull(11, Types.VARCHAR);
            else st.setString(11, searchLog.getBlenderName());

            if (searchLog.getJsonReturned() == null) st.setObject(12, null);
            else st.setObject(12, createJsonbPGobject(searchLog.getJsonReturned()));

            if (searchLog.getJsonData() == null) st.setObject(13, null);
            else st.setObject(13, createJsonbPGobject(searchLog.getJsonData()));

            if (searchLog.getJsonCandidates() == null) st.setObject(14, null);
            else st.setObject(14, createJsonbPGobject(searchLog.getJsonCandidates()));

            if (searchLog.getJsonReferences() == null) st.setObject(15, null);
            else st.setObject(15, createJsonbPGobject(searchLog.getJsonReferences()));

            if (searchLog.getJsonAttributes() == null) st.setObject(16, null);
            else st.setObject(16, createJsonbPGobject(searchLog.getJsonAttributes()));

            if (searchLog.getJsonFeedback() == null) st.setObject(17, null);
            else st.setObject(17, createJsonbPGobject(searchLog.getJsonFeedback()));

            st.execute();
            searchLog.setId(st.getLong(1));
        } catch (SQLException e) {
            final String reason = "Failed to insert a LogSvcSearch: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }
}
