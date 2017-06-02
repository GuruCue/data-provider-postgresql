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
import com.gurucue.recommendations.entitymanager.LogSvcRecommendationManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Implementation of the Recommendations Logging Manager interface for PostgreSQL.
 */
public final class LogSvcRecommendationManagerImpl implements LogSvcRecommendationManager {
    private static final Logger log = LogManager.getLogger(LogSvcRecommendationManagerImpl.class);
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    private final PostgreSqlDataLink dataLink;

    LogSvcRecommendationManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
    }

    @Override
    public void put(final LogSvcRecommendation recLog) {
        if (recLog == null) {
            log.error("Attempted to save a null recommendation log");
            return;
        }
        final Calendar cal = Calendar.getInstance();
        cal.setTimeZone(utcTimeZone);
        try (final CallableStatement st = dataLink.prepareCall(
                "{ ? = call log_svc_rec_partition_insert_function(null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }"
        )) {
            st.registerOutParameter(1, Types.BIGINT);
            st.setLong(2, recLog.getPartner().getId());
            if (recLog.getConsumerId() > 0L) st.setLong(3, recLog.getConsumerId());
            else st.setNull(3, Types.BIGINT);
            if (recLog.getMaxRecommendations() == null) st.setNull(4, Types.INTEGER);
            else st.setInt(4, recLog.getMaxRecommendations());
            st.setInt(5, recLog.getResponseCode());
            if (recLog.getFailureCondition() == null) st.setNull(6, Types.VARCHAR);
            else st.setString(6, recLog.getFailureCondition());
            if (recLog.getFailedRequest() == null) st.setNull(7, Types.VARCHAR);
            else st.setString(7, recLog.getFailedRequest());
            st.setTimestamp(8, recLog.getRequestTimestamp(), cal);
            if (recLog.getRequestDuration() == null) st.setNull(9, Types.BIGINT);
            else st.setLong(9, recLog.getRequestDuration());
            if (recLog.getPartnerRecommenderName() == null) st.setNull(10, Types.VARCHAR);
            else st.setString(10, recLog.getPartnerRecommenderName());

            if (recLog.getBlenderName() == null) st.setNull(11, Types.VARCHAR);
            else st.setString(11, recLog.getBlenderName());

            if (recLog.getJsonCandidates() == null) st.setObject(12,null);
            else st.setObject(12, createJsonbPGobject(recLog.getJsonCandidates()));

            if (recLog.getJsonReturned() == null) st.setObject(13, null);
            else st.setObject(13, createJsonbPGobject(recLog.getJsonReturned()));

            if (recLog.getJsonReferences() == null) st.setObject(14, null);
            else st.setObject(14, createJsonbPGobject(recLog.getJsonReferences()));

            if (recLog.getJsonData() == null) st.setObject(15, null);
            else st.setObject(15, createJsonbPGobject(recLog.getJsonData()));

            if (recLog.getJsonAttributes() == null) st.setObject(16, null);
            else st.setObject(16, createJsonbPGobject(recLog.getJsonAttributes()));

            if (recLog.getJsonFeedback() == null) st.setObject(17, null);
            else st.setObject(17, createJsonbPGobject(recLog.getJsonFeedback()));

            st.execute();
            recLog.setId(st.getLong(1));
        }
        catch (SQLException e) {
            final String reason = "Failed to insert a LogSvcRec: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    private PGobject createJsonbPGobject(final String json) throws SQLException {
        final PGobject result = new PGobject();
        result.setType("jsonb");
        result.setValue(json);
        return result;
    }
}
