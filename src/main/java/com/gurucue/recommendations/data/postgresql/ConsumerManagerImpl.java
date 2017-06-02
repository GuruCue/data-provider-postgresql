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

import com.google.common.collect.ImmutableMap;
import com.gurucue.recommendations.DatabaseException;
import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.dto.ConsumerEntity;
import com.gurucue.recommendations.dto.RelationConsumerProductEntity;
import com.gurucue.recommendations.entitymanager.ConsumerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Implementation of the Consumer Manager interface for PostgreSQL.
 */
final class ConsumerManagerImpl implements ConsumerManager {
    private static final Logger log = LogManager.getLogger(ConsumerManagerImpl.class);
    private static final ImmutableMap<String, Long> emptyChildren = ImmutableMap.of();
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    private final PostgreSqlDataLink dataLink;
    private final Calendar utcCalendar;

    ConsumerManagerImpl(final PostgreSqlDataLink dataLink) {
        this.dataLink = dataLink;
        utcCalendar = Calendar.getInstance(utcTimeZone);
        utcCalendar.setTimeZone(utcTimeZone);
    }

    @Override
    public ConsumerEntity getById(final long id) {
        return internalGetById(id, false);
    }

    private static final String SELECT_CONSUMER_BY_ID = "select username, partner_id, activated, deleted, status, type_id, parent_id from consumer where id = ? and status = 1";
    private static final String SELECT_CONSUMER_BY_ID_FOR_UPDATE = SELECT_CONSUMER_BY_ID + " for update";
    private ConsumerEntity internalGetById(final long id, final boolean forUpdate) {
        try (final PreparedStatement st = dataLink.prepareStatement(forUpdate ? SELECT_CONSUMER_BY_ID_FOR_UPDATE : SELECT_CONSUMER_BY_ID)) {
            st.setLong(1, id);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                String username = rs.getString(1);
                if (rs.wasNull()) username = null;
                long partnerId = rs.getLong(2);
                if (rs.wasNull()) partnerId = -1L;
                Timestamp activated = rs.getTimestamp(3, utcCalendar);
                if (rs.wasNull()) activated = null;
                Timestamp deleted = rs.getTimestamp(4, utcCalendar);
                if (rs.wasNull()) deleted = null;
                long consumerTypeId = rs.getLong(6);
                if (rs.wasNull()) consumerTypeId = 0L;
                long parentConsumerId = rs.getLong(7);
                if (rs.wasNull()) parentConsumerId = 0L;
                return new ConsumerEntity(id, username, partnerId, activated == null ? -1L: activated.getTime(), deleted == null ? -1L : deleted.getTime(), rs.getShort(5), getRelations(id, forUpdate), consumerTypeId, parentConsumerId);
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to retrieve consumer with ID " + id + ": " + e.toString(), e);
        }
    }

    @Override
    public ConsumerEntity getByPartnerIdAndUsernameAndTypeAndParent(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId) {
        return internalGetByPartnerIdAndUsernameAndTypeAndParent(partnerId, username, consumerTypeId, parentConsumerId, false);
    }

    @Override
    public List<ConsumerEntity> list() {
        log.debug("Reading a list of consumers");
        final Calendar cal = utcCalendar;
        final List<FillHelper> consumers = new ArrayList<>(200000);
        try (final Statement st = dataLink.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(5000);
            try (final ResultSet rs = st.executeQuery("select id, username, partner_id, activated, deleted, status, type_id, parent_id from consumer where status > 0 order by id")) {
                while (rs.next()) {
                    final long id = rs.getLong(1);
                    String username = rs.getString(2);
                    if (rs.wasNull()) username = null;
                    long partnerId = rs.getLong(3);
                    if (rs.wasNull()) partnerId = -1L;
                    Timestamp activated = rs.getTimestamp(4, cal);
                    if (rs.wasNull()) activated = null;
                    Timestamp deleted = rs.getTimestamp(5, cal);
                    if (rs.wasNull()) deleted = null;
                    long consumerTypeId = rs.getLong(7);
                    if (rs.wasNull()) consumerTypeId = 0L;
                    long parentConsumerId = rs.getLong(8);
                    if (rs.wasNull()) parentConsumerId = 0L;
                    final FillHelper fh = new FillHelper(id, username, partnerId, activated == null ? -1L : activated.getTime(), deleted == null ? -1L : deleted.getTime(), rs.getShort(6), consumerTypeId, parentConsumerId);
                    consumers.add(fh);
                }
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("", e);
        }

        final int consumerCount = consumers.size();
        if (consumerCount > 0) {
            log.debug("Reading consumer relations to add to the list of consumers");
            try (final Statement st = dataLink.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                st.setFetchSize(5000);
                try (final ResultSet rs = st.executeQuery("select r.id, r.consumer_id, r.product_id, r.relation_type_id, r.relation_start, r.relation_end from relation_consumer_product r inner join consumer c on c.id = r.consumer_id where c.status > 0 order by r.consumer_id")) {
                    int consumerIndex = 0;
                    FillHelper currentConsumer = consumers.get(consumerIndex);
                    long currentConsumerId = currentConsumer.id;
                    readLoop:
                    while (rs.next()) {
                        final long consumerId = rs.getLong(2);
                        while (currentConsumerId < consumerId) {
                            consumerIndex++;
                            if (consumerIndex >= consumerCount) {
                                log.error("Overflow: last read consumer ID was " + currentConsumerId + ", but a relation references consumer ID " + consumerId);
                                break readLoop;
                            }
                            currentConsumer = consumers.get(consumerIndex);
                            currentConsumerId = currentConsumer.id;
                        }
                        if (consumerId < currentConsumerId) {
                            log.error("Read a relation for consumer ID " + consumerId + ", but it was not in the list of consumers");
                        }
                        else {
                            long productId = rs.getLong(3);
                            if (rs.wasNull()) productId = -1L;
                            long relationTypeId = rs.getLong(4);
                            if (rs.wasNull()) relationTypeId = -1L;
                            Timestamp t = rs.getTimestamp(5, cal);
                            long relationStart = rs.wasNull() || (t == null) ? -1L : t.getTime();
                            t = rs.getTimestamp(6, cal);
                            long relationEnd = rs.wasNull() || (t == null) ? -1L : t.getTime();
                            currentConsumer.relations.add(new RelationConsumerProductEntity(rs.getLong(1), consumerId, productId, relationTypeId, relationStart, relationEnd));
                        }
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("", e);
            }
        }

        final List<ConsumerEntity> result = new ArrayList<>();
        final ImmutableMap<String, Long> ec = emptyChildren; // local variable access is faster
        for (final FillHelper fh : consumers) {
            result.add(fh.asConsumer());
        }
        consumers.clear();
        log.debug("Returning " + consumerCount + " consumers");
        return result;
    }

    private static final String SELECT_CONSUMER_BY_PARTNERID_AND_USERNAME = "select id, activated, deleted from consumer where partner_id = ? and username = ? and status = 1 and type_id = ? and parent_id = ?";
    private static final String SELECT_CONSUMER_BY_PARTNERID_AND_USERNAME_FOR_UPDATE = SELECT_CONSUMER_BY_PARTNERID_AND_USERNAME + " for update";

    private ConsumerEntity internalGetByPartnerIdAndUsernameAndTypeAndParent(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId, final boolean forUpdate) {
        try (final PreparedStatement st = dataLink.prepareStatement(forUpdate ? SELECT_CONSUMER_BY_PARTNERID_AND_USERNAME_FOR_UPDATE : SELECT_CONSUMER_BY_PARTNERID_AND_USERNAME, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setLong(1, partnerId);
            st.setString(2, username);
            st.setLong(3, consumerTypeId);
            if (parentConsumerId == 0L) st.setNull(4, Types.BIGINT);
            else st.setLong(4, parentConsumerId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return null;
                final long id = rs.getLong(1);
                Timestamp activated = rs.getTimestamp(2, utcCalendar);
                if (rs.wasNull()) activated = null;
                Timestamp deleted = rs.getTimestamp(3, utcCalendar);
                if (rs.wasNull()) deleted = null;
                return new ConsumerEntity(id, username, partnerId, activated == null ? -1L: activated.getTime(), deleted == null ? -1L : deleted.getTime(), (short)1, getRelations(id, forUpdate), consumerTypeId, parentConsumerId);
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to retrieve for partner ID " + partnerId + " consumer with username \"" + username.replace("\"", "\\\"") + "\": " + e.toString(), e);
        }
    }

    private SelectOrCreateInfo selectOrCreate(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId) throws SQLException {
        final SelectOrCreateInfo result;
        try (final PreparedStatement st = dataLink.prepareStatement("select id, activated, is_new from select_or_create_consumer(?, ?, ?, ?)")) {
            st.setLong(1, partnerId);
            st.setString(2, username);
            st.setLong(3, consumerTypeId);
            if (parentConsumerId == 0L) st.setNull(4, Types.BIGINT);
            else st.setLong(4, parentConsumerId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) throw new DatabaseException("The SQL function select_or_create_consumer() did not return a row for partner ID " + partnerId + ", username \"" + username.replace("\"", "\\\"") + "\", consumer type ID " + consumerTypeId + " and parent consumer ID " + parentConsumerId);
                final Timestamp a = rs.getTimestamp(2, utcCalendar);
                final long activated;
                if (rs.wasNull() || (a == null)) activated = -1L;
                else activated = a.getTime();
                result = new SelectOrCreateInfo(rs.getLong(1), activated, -1L, rs.getShort(3) != 0);
            }
        }

        if (result.isNew && (consumerTypeId == 1L)) {
            // a new subscription profile requires a default user profile
            try (final PreparedStatement st = dataLink.prepareStatement("insert into consumer (username, partner_id, activated, status, type_id, parent_id) values ('', ?, ?, 1, 2, ?)")) {
                st.setLong(1, partnerId);
                st.setTimestamp(2, new Timestamp(result.activated), utcCalendar);
                st.setLong(3, result.consumerId);
                st.executeUpdate();
            }
        }

        return result;
    }

    private List<RelationConsumerProductEntity> insertRelations(final long consumerId, final List<RelationConsumerProductEntity> relations) throws SQLException {
        if ((relations == null) || (relations.size() == 0)) return Collections.emptyList();
        final List<RelationConsumerProductEntity> newRelations = new ArrayList<>();
        final Calendar cal = utcCalendar;
        try (final PreparedStatement st = dataLink.prepareStatement("insert into relation_consumer_product (consumer_id, product_id, relation_type_id, relation_start, relation_end) values (?, ?, ?, ?, ?) returning id")) {
            for (final RelationConsumerProductEntity r : relations) {
                st.setLong(1, consumerId);
                st.setLong(2, r.productId);
                st.setLong(3, r.relationTypeId);
                if (r.relationStart < 0) st.setNull(4, Types.TIMESTAMP);
                else st.setTimestamp(4, new Timestamp(r.relationStart), cal);
                if (r.relationEnd < 0) st.setNull(5, Types.TIMESTAMP);
                else st.setTimestamp(5, new Timestamp(r.relationEnd), cal);
                try (final ResultSet rs = st.executeQuery()) {
                    rs.next();
                    newRelations.add(new RelationConsumerProductEntity(rs.getLong(1), consumerId, r.productId, r.relationTypeId, r.relationStart, r.relationEnd));
                }
            }
        }
        return newRelations;
    }

    // TODO: on reset copy the whole cascade (hierarchy) of active consumer children
    private SelectOrCreateInfo resetConsumer(final long partnerId, final long consumerId, final String username, final long consumerTypeId, final long parentConsumerId) throws SQLException {
        final long nowSeconds = System.currentTimeMillis() / 1000L;
        final Timestamp now = new Timestamp(nowSeconds * 1000L); // round to a second
        // first make the current record old
        try (final PreparedStatement st = dataLink.prepareStatement("update consumer set status = 0, deleted = ? where id = ?")) {
            st.setTimestamp(1, now, utcCalendar);
            st.setLong(2, consumerId);
            st.executeUpdate();
        }
        // now create the new record
        final SelectOrCreateInfo info;
        try (final PreparedStatement st = dataLink.prepareStatement("insert into consumer (username, partner_id, activated, status, type_id, parent_id) values (?, ?, ?, 1, ?, ?) returning id, activated")) {
            st.setString(1, username);
            st.setLong(2, partnerId);
            st.setTimestamp(3, now, utcCalendar);
            st.setLong(4, consumerTypeId);
            if (parentConsumerId == 0L) st.setNull(5, Types.BIGINT);
            else st.setLong(5, parentConsumerId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) throw new DatabaseException("New consumer insertion (after deactivating the previous record) returned no row");
                final Timestamp a = rs.getTimestamp(2, utcCalendar);
                final long activated;
                if (rs.wasNull() || (a == null)) activated = -1L;
                else activated = a.getTime();
                info = new SelectOrCreateInfo(rs.getLong(1), activated, -1L, false);
            }
        }
        // copy all active user profiles
        try (final PreparedStatement st = dataLink.prepareStatement("insert into consumer (username, partner_id, activated, status, type_id, parent_id) select username, partner_id, ?, status, type_id, ? from consumer where parent_id = ? and status = 1")) {
            st.setTimestamp(1, now, utcCalendar);
            st.setLong(2, info.consumerId);
            st.setLong(3, consumerId);
            st.executeUpdate();
        }
        // terminate all active user profiles of the previous consumer instance
        deleteAllChildren(consumerId, now);
        // and copy all the subscriptions of the previous instance to the new instance
        try (final PreparedStatement st = dataLink.prepareStatement(
                "insert into relation_consumer_product (consumer_id, product_id, relation_type_id, relation_start, relation_end) " +
                "select ?, product_id, relation_type_id, relation_start, relation_end from relation_consumer_product where consumer_id = ? and deleted is not null"
        )) {
            st.setLong(1, info.consumerId);
            st.setLong(2, consumerId);
            st.executeUpdate();
        }
        // use new values onwards
        return info;
    }

    @Override
    public ConsumerEntity merge(final long partnerId, final String username, final boolean resetEvents, final List<RelationConsumerProductEntity> relations, final long consumerTypeId, final long parentConsumerId) {
        try {
            final SelectOrCreateInfo initialSoci = selectOrCreate(partnerId, username, consumerTypeId, parentConsumerId);
            long consumerId = initialSoci.consumerId;
            long activated = initialSoci.activated;
            final boolean isNew = initialSoci.isNew;

            if (isNew) {
                // anonymize any old data
                anonymizeNow(partnerId, username, consumerTypeId, parentConsumerId);
                // shortcut, in case this is a brand new consumer
                if ((relations == null) || relations.isEmpty()) return new ConsumerEntity(consumerId, username, partnerId, activated, -1L, (short)1, Collections.<RelationConsumerProductEntity>emptyList(), consumerTypeId, parentConsumerId);
                final List<RelationConsumerProductEntity> newRelations = new ArrayList<>();
                final Calendar cal = utcCalendar;
                try (final PreparedStatement st = dataLink.prepareStatement("insert into relation_consumer_product (consumer_id, product_id, relation_type_id, relation_start, relation_end) values (?, ?, ?, ?, ?) returning id")) {
                    for (final RelationConsumerProductEntity r : relations) {
                        st.setLong(1, consumerId);
                        st.setLong(2, r.productId);
                        st.setLong(3, r.relationTypeId);
                        if (r.relationStart < 0) st.setNull(4, Types.TIMESTAMP);
                        else st.setTimestamp(4, new Timestamp(r.relationStart), cal);
                        if (r.relationEnd < 0) st.setNull(5, Types.TIMESTAMP);
                        else st.setTimestamp(5, new Timestamp(r.relationEnd), cal);
                        try (final ResultSet rs = st.executeQuery()) {
                            rs.next();
                            newRelations.add(new RelationConsumerProductEntity(rs.getLong(1), consumerId, r.productId, r.relationTypeId, r.relationStart, r.relationEnd));
                        }
                    }
                }
                return new ConsumerEntity(consumerId, username, partnerId, activated, -1L, (short)1, newRelations, consumerTypeId, parentConsumerId);
            }

            // the consumer already exists
            // no further concurrency issues exist, because we hold exclusive lock of the current record in the database, guaranteed by "select...for update" inside the select_or_create_consumer() SQL function
            // first read the current list of relations
            if (resetEvents) {
                // create a new consumer entry to break the event chain
                final SelectOrCreateInfo resetSoci = resetConsumer(partnerId, consumerId, username, consumerTypeId, parentConsumerId);
                // use new values onwards
                consumerId = resetSoci.consumerId;
                activated = resetSoci.activated;
            }

            if ((relations != null) && (relations.size() > 0)) {
                // merge the relations
                final Calendar cal = utcCalendar;
                try (final PreparedStatement stUpdate = dataLink.prepareStatement(
                        "update relation_consumer_product set relation_start = ?, relation_end = ? where consumer_id = ? and product_id = ? and relation_type_id = ? returning id"
                )) {
                    try (final PreparedStatement stInsert = dataLink.prepareStatement(
                            "insert into relation_consumer_product (consumer_id, product_id, relation_type_id, relation_start, relation_end) values (?, ?, ?, ?, ?)"
                    )) {
                        for (final RelationConsumerProductEntity r : relations) {
                            final Timestamp startTimestamp = r.relationStart < 0L ? null : new Timestamp(r.relationStart);
                            final Timestamp endTimestamp = r.relationEnd < 0L ? null : new Timestamp(r.relationEnd);
                            if (startTimestamp == null) stUpdate.setNull(1, Types.TIMESTAMP);
                            else stUpdate.setTimestamp(1, startTimestamp, cal);
                            if (endTimestamp == null) stUpdate.setNull(2, Types.TIMESTAMP);
                            else stUpdate.setTimestamp(2, endTimestamp, cal);
                            stUpdate.setLong(3, consumerId);
                            stUpdate.setLong(4, r.productId);
                            stUpdate.setLong(5, r.relationTypeId);
                            try (final ResultSet rs = stUpdate.executeQuery()) {
                                if (!rs.next()) {
                                    // update returned no rows, insert is needed
                                    stInsert.setLong(1, consumerId);
                                    stInsert.setLong(2, r.productId);
                                    stInsert.setLong(3, r.relationTypeId);
                                    if (startTimestamp == null) stInsert.setNull(4, Types.TIMESTAMP);
                                    else stInsert.setTimestamp(4, startTimestamp, cal);
                                    if (endTimestamp == null) stInsert.setNull(5, Types.TIMESTAMP);
                                    else stInsert.setTimestamp(5, endTimestamp, cal);
                                    stInsert.executeUpdate();
                                }
                            }
                        }
                    }
                }
            }

            return internalGetById(consumerId, false);
        }
        catch (SQLException e) {
            if (resetEvents) throw new DatabaseException("Failed to create or update a consumer with reset events for partner ID " + partnerId + " with username \"" + username.replace("\"", "\\\"") + ": " + e.toString(), e);
            throw new DatabaseException("Failed to create or update a consumer for partner ID " + partnerId + " with username \"" + username.replace("\"", "\\\"") + ": " + e.toString(), e);
        }
    }

    private Map<RelationKey, List<RelationConsumerProductEntity>> mapRelations(final List<RelationConsumerProductEntity> relations) {
        if ((relations == null) || relations.isEmpty()) return Collections.emptyMap();
        final Map<RelationKey, List<RelationConsumerProductEntity>> map = new HashMap<>();
        listProcessing:
        for (final RelationConsumerProductEntity relation : relations) {
            final RelationKey key = new RelationKey(relation.productId, relation.relationTypeId);
            final List<RelationConsumerProductEntity> relationList = map.get(key);
            if (relationList == null) {
                final List<RelationConsumerProductEntity> newRelationList = new ArrayList<>();
                map.put(key, newRelationList);
                newRelationList.add(relation);
                continue;
            }
            // sorted insertion into an existing list
            final int listSize = relationList.size();
            int i = 0;
            RelationConsumerProductEntity r = relationList.get(0); // it is guaranteed that if a list exists, it is not empty
            // scan until the first relation with the same or bigger start-time is found
            final long relationStart = relation.relationStart;
            while (r.relationStart < relationStart) {
                i++;
                if (i >= listSize) {
                    // no more items in the list, append and continue with the next relation
                    relationList.add(relation);
                    continue listProcessing;
                }
                r = relationList.get(i);
            }
            // scan until the first relation with the same start-time and bigger end-time
            final long relationEnd = relation.relationEnd;
            while ((r.relationStart == relationStart) && (r.relationEnd <= relationEnd)) {
                i++;
                if (i >= listSize) {
                    // no more items in the list, append and continue with the next relation
                    relationList.add(relation);
                    continue listProcessing;
                }
                r = relationList.get(i);
            }
            // we have the first relation with the same start-time and a bigger end-time, or with a bigger start-time, insert before it
            relationList.add(i, relation);
        }
        return map;
    }

    private static final String INSERT_RELATION_SQL = "insert into relation_consumer_product (consumer_id, product_id, relation_type_id, relation_start, relation_end) values (?, ?, ?, ?, ?) returning id";

    /**
     * Inserts a new relation. The given statement must be prepared with the SQL
     * in {@link #INSERT_RELATION_SQL}.
     *
     * @param insertRelation
     * @param consumerId
     * @param newRelation
     * @return
     * @throws SQLException
     */
    private RelationConsumerProductEntity insertRelation(final PreparedStatement insertRelation, final long consumerId, final RelationConsumerProductEntity newRelation) throws SQLException {
        insertRelation.setLong(1, consumerId);
        insertRelation.setLong(2, newRelation.productId);
        insertRelation.setLong(3, newRelation.relationTypeId);
        if (newRelation.relationStart < 0L) insertRelation.setNull(4, Types.TIMESTAMP);
        else insertRelation.setTimestamp(4, new Timestamp(newRelation.relationStart), utcCalendar);
        if (newRelation.relationEnd < 0L) insertRelation.setNull(5, Types.TIMESTAMP);
        else insertRelation.setTimestamp(5, new Timestamp(newRelation.relationEnd), utcCalendar);
        final long id;
        try (final ResultSet rs = insertRelation.executeQuery()) {
            if (!rs.next()) throw new DatabaseException("Could not retrieve ID of the newly inserted consumer relation");
            id = rs.getLong(1);
        }
        return new RelationConsumerProductEntity(id, consumerId, newRelation.productId, newRelation.relationTypeId, newRelation.relationStart, newRelation.relationEnd);
    }

    private static final String UPDATE_RELATION_SQL = "update relation_consumer_product set relation_start = ?, relation_end = ? where id = ?";

    private void updateRelationTimes(final PreparedStatement updateRelation, final RelationConsumerProductEntity relation) throws SQLException {
        if (relation.relationStart < 0L) updateRelation.setNull(1, Types.TIMESTAMP);
        else updateRelation.setTimestamp(1, new Timestamp(relation.relationStart), utcCalendar);
        if (relation.relationEnd < 0L) updateRelation.setNull(2, Types.TIMESTAMP);
        else updateRelation.setTimestamp(2, new Timestamp(relation.relationEnd), utcCalendar);
        updateRelation.setLong(3, relation.id);
        updateRelation.executeUpdate();
    }

    private static final String DELETE_RELATION_SQL = "delete from relation_consumer_product where id = ?";

    private void deleteRelation(final PreparedStatement deleteRelation, final long relationId) throws SQLException {
        deleteRelation.setLong(1, relationId);
        deleteRelation.executeUpdate();
    }

    // TODO: finish it
    @Override
    public ConsumerEntity update(final long partnerId, final String username, final boolean resetEvents, final List<RelationConsumerProductEntity> newRelations, final long consumerTypeId, final long parentConsumerId) {
        try {
            SelectOrCreateInfo soci = selectOrCreate(partnerId, username, consumerTypeId, parentConsumerId); // this always returns a consumer with status = 1
            if (soci.isNew) {
                // anonymize any old data
                anonymizeNow(partnerId, username, consumerTypeId, parentConsumerId);
                // there is no such a consumer yet, create a new one and return it
                return new ConsumerEntity(soci.consumerId, username, partnerId, soci.activated, soci.deleted, (short)1, insertRelations(soci.consumerId, newRelations), consumerTypeId, parentConsumerId);
            }
            // if resetEvents, then create a new consumer instance with the same username, copying any relations
            if (resetEvents) {
                soci = resetConsumer(partnerId, soci.consumerId, username, consumerTypeId, parentConsumerId);
            }
            // retrieve existing relations
            final Map<RelationKey, List<RelationConsumerProductEntity>> existingRelations = getMappedRelations(soci.consumerId, true);
            final Set<RelationKey> remainingKeys = new HashSet<>(existingRelations.keySet());
            // now replace the state of relations
            final long now = (Timer.currentTimeMillis() / 1000L) * 1000L; // the resolution is 1 second
            PreparedStatement insertRelation = null;
            PreparedStatement updateRelation = null;
            PreparedStatement deleteRelation = null;
            try {
                if ((newRelations != null) && !newRelations.isEmpty()) {
                    nextNewRelation:
                    for (RelationConsumerProductEntity newRelation : newRelations) {
                        if (newRelation.relationStart < 0L) {
                            newRelation = new RelationConsumerProductEntity(newRelation.id, newRelation.consumerId, newRelation.productId, newRelation.relationTypeId, now, newRelation.relationEnd);
                        }
                        final RelationKey currentKey = new RelationKey(newRelation.productId, newRelation.relationTypeId);
                        remainingKeys.remove(currentKey);
                        final List<RelationConsumerProductEntity> existingRelationList = existingRelations.get(currentKey);
                        final List<RelationConsumerProductEntity> newList = new ArrayList<>();
                        if ((existingRelationList == null) || (existingRelationList.size() == 0)) {
                            // a new relation, do an insert
                            if (insertRelation == null) insertRelation = dataLink.prepareStatement(INSERT_RELATION_SQL);
                            newList.add(insertRelation(insertRelation, soci.consumerId, newRelation));
                        } else {
                            // at least one relation exists already
                            final long relationStart = newRelation.relationStart;
                            final Iterator<RelationConsumerProductEntity> relationIterator = existingRelationList.iterator();
                            RelationConsumerProductEntity existingRelation = null;
                            // find a relation to modify
                            while (relationIterator.hasNext()) {
                                existingRelation = relationIterator.next();
                                if (existingRelation.relationEnd < 0L) {
                                    // this should be the last relation
                                    break; // this will remove any remaining relations
                                }
                                if (existingRelation.relationEnd < relationStart) {
                                    // this relation is history
                                    newList.add(existingRelation);
                                    existingRelation = null; // in case this is the last iteration
                                    continue;
                                }
                                // now we know that the current relation ends after the new relation starts
                                if (existingRelation.relationStart >= relationStart) {
                                    // remove this relation
                                    if (deleteRelation == null)
                                        deleteRelation = dataLink.prepareStatement(DELETE_RELATION_SQL);
                                    deleteRelation(deleteRelation, existingRelation.id);
                                    existingRelation = null;
                                    break; // any trailing relation will be removed, because it comes after this one, which is already in the future
                                }
                                // this should be the last relation
                                break; // this will remove any remaining relations
                            }
                            // remove any trailing relations, they are in the future
                            while (relationIterator.hasNext()) {
                                final RelationConsumerProductEntity r = relationIterator.next();
                                if (deleteRelation == null)
                                    deleteRelation = dataLink.prepareStatement(DELETE_RELATION_SQL);
                                deleteRelation(deleteRelation, r.id);
                            }
                            // modify the existing relation
                            RelationConsumerProductEntity r;
                            if (existingRelation == null) {
                                if (insertRelation == null)
                                    insertRelation = dataLink.prepareStatement(INSERT_RELATION_SQL);
                                r = insertRelation(insertRelation, soci.consumerId, newRelation);
                            } else if (existingRelation.relationEnd == newRelation.relationEnd) {
                                // no change necessary
                                r = existingRelation;
                            } else {
                                if (updateRelation == null)
                                    updateRelation = dataLink.prepareStatement(UPDATE_RELATION_SQL);
                                r = new RelationConsumerProductEntity(existingRelation.id, existingRelation.consumerId, existingRelation.productId, existingRelation.relationTypeId, existingRelation.relationStart, newRelation.relationEnd);
                                updateRelationTimes(updateRelation, r);
                            }
                            newList.add(r);
                        }
                        existingRelations.put(currentKey, newList); // replace with the new state
                    }
                }

                // terminate any remaining subscriptions
                for (final RelationKey key : remainingKeys) {
                    final List<RelationConsumerProductEntity> existingRelationList = existingRelations.get(key);
                    final List<RelationConsumerProductEntity> newList = new ArrayList<>();
                    final Iterator<RelationConsumerProductEntity> relationIterator = existingRelationList.iterator();
                    RelationConsumerProductEntity existingRelation = null;
                    // find a relation to modify
                    while (relationIterator.hasNext()) {
                        existingRelation = relationIterator.next();
                        if (existingRelation.relationEnd < 0L) {
                            // this should be the last relation
                            break; // this will remove any remaining relations
                        }
                        if (existingRelation.relationEnd < now) {
                            // this relation is history
                            newList.add(existingRelation);
                            existingRelation = null; // in case this is the last iteration
                            continue;
                        }
                        // now we know that the current relation ends after the new relation starts
                        if (existingRelation.relationStart >= now) {
                            // remove this relation
                            if (deleteRelation == null) deleteRelation = dataLink.prepareStatement(DELETE_RELATION_SQL);
                            deleteRelation(deleteRelation, existingRelation.id);
                            existingRelation = null;
                            break; // any trailing relation will be removed, because it comes after this one, which is already in the future
                        }
                        // this should be the last relation
                        break; // this will remove any remaining relations
                    }
                    // remove any trailing relations, they are in the future
                    while (relationIterator.hasNext()) {
                        final RelationConsumerProductEntity r = relationIterator.next();
                        if (deleteRelation == null) deleteRelation = dataLink.prepareStatement(DELETE_RELATION_SQL);
                        deleteRelation(deleteRelation, r.id);
                    }
                    // modify the last relation if necessary
                    if (existingRelation != null) {
                        if (updateRelation == null) updateRelation = dataLink.prepareStatement(UPDATE_RELATION_SQL);
                        final RelationConsumerProductEntity r = new RelationConsumerProductEntity(existingRelation.id, existingRelation.consumerId, existingRelation.productId, existingRelation.relationTypeId, existingRelation.relationStart, now);
                        updateRelationTimes(updateRelation, r);
                        newList.add(r);
                    }
                    existingRelations.put(key, newList); // replace with the new state
                }
            }
            finally {
                if (insertRelation != null) {
                    try {
                        insertRelation.close();
                    }
                    catch (SQLException e) {
                        throw new DatabaseException("Failed to close the insert statement: " + e.toString(), e);
                    }
                }
                if (updateRelation != null) {
                    try {
                        updateRelation.close();
                    }
                    catch (SQLException e) {
                        throw new DatabaseException("Failed to close the update statement: " + e.toString(), e);
                    }
                }
                if (deleteRelation != null) {
                    try {
                        deleteRelation.close();
                    }
                    catch (SQLException e) {
                        throw new DatabaseException("Failed to close the delete statement: " + e.toString(), e);
                    }
                }
            }
            // now collect the refreshed relations
            final List<RelationConsumerProductEntity> resultRelations = new ArrayList<>();
            for (final List<RelationConsumerProductEntity> r : existingRelations.values()) {
                if (r != null) resultRelations.addAll(r);
            }
            // lastly, return the new consumer
            return new ConsumerEntity(soci.consumerId, username, partnerId, soci.activated, soci.deleted, (short)1, resultRelations, consumerTypeId, parentConsumerId);
        }
        catch (SQLException e) {
            if (resetEvents) throw new DatabaseException("Failed to create or update a consumer with reset events for partner ID " + partnerId + " with username \"" + username.replace("\"", "\\\"") + ": " + e.toString(), e);
            throw new DatabaseException("Failed to create or update a consumer for partner ID " + partnerId + " with username \"" + username.replace("\"", "\\\"") + ": " + e.toString(), e);
        }
    }

    private void deleteAllChildren(final long parentConsumerId, final Timestamp deletionTimestamp) throws SQLException {
        final LinkedList<Long> queue = new LinkedList<>();
        queue.add(parentConsumerId);
        final Calendar cal = utcCalendar;
        try (final PreparedStatement st = dataLink.prepareStatement("update consumer set status = 0, deleted = ? where parent_id = ? and status = 1 returning id")) {
            while (!queue.isEmpty()) {
                st.setTimestamp(1, deletionTimestamp, cal);
                st.setLong(2, queue.removeFirst());
                try (final ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        queue.add(rs.getLong(1));
                    }
                }
            }
        }
    }

    @Override
    public ConsumerEntity delete(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId, final boolean delayedAnonymization) {
        ConsumerEntity result = null;
        final long startTime = System.nanoTime();

        if (delayedAnonymization) {
            // anonymize any old records
            anonymizeNow(partnerId, username, consumerTypeId, parentConsumerId);
            final long anonTime = System.nanoTime();

            // set all active records to delayed anonymization
            final StringBuilder sqlBuilder = new StringBuilder(128);
            sqlBuilder.append("update consumer set status = 2, deleted = current_timestamp at time zone 'utc' where status = 1 and partner_id = ? and username = ? and type_id = ? and parent_id ");
            if (parentConsumerId == 0L) sqlBuilder.append("is null"); // there's a bug in PostgreSQL: it doesn't handle correctly NULL values in parameters
            else sqlBuilder.append("= ?");
            sqlBuilder.append(" returning id, deleted, activated");
            try (final PreparedStatement st = dataLink.prepareStatement(sqlBuilder.toString())) { // "update consumer set status = 2, deleted = current_timestamp at time zone 'utc' where status = 1 and partner_id = ? and username = ? and type_id = ? and parent_id = ? returning id, deleted, activated"
                st.setLong(1, partnerId);
                st.setString(2, username);
                st.setLong(3, consumerTypeId);
                if (parentConsumerId != 0L) st.setLong(4, parentConsumerId);
                try (final ResultSet rs = st.executeQuery()) {
                    if (rs.next()) {
                        final long consumerId = rs.getLong(1);
                        Timestamp deleted = rs.getTimestamp(2, utcCalendar);
                        if (rs.wasNull()) deleted = null;
                        Timestamp activated = rs.getTimestamp(3, utcCalendar);
                        if (rs.wasNull()) activated = null;
                        // do a cascade deletion of children
                        deleteAllChildren(consumerId, deleted);
                        // delay one day
                        final long delayedTime = (deleted == null ? Timer.currentTimeMillis() : deleted.getTime()) + 86400000L;
                        Timer.INSTANCE.schedule(delayedTime, new ConsumerAnonymization(dataLink.provider, consumerId, delayedTime));
                        result = new ConsumerEntity(consumerId, username, partnerId, activated == null ? -1L : activated.getTime(), deleted == null ? -1L : deleted.getTime(), (short) 2, getRelations(consumerId, false), consumerTypeId, parentConsumerId);
                    }
                    else {
                        log.warn("No consumer deleted: partner_id=" + partnerId + " and username='" + username + "' and type_id=" + consumerTypeId + " and parent_id=" + parentConsumerId + ", delayedAnonymization=true");
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("Failed to set delayed anonymization for consumer \"" + username.replace("\"", "\\\"") + "\" and partner ID " + partnerId + ": " + e.toString(), e);
            }
            final long deleteTime = System.nanoTime();

            log.debug(new StringBuilder(250)
                    .append("[")
                    .append(Thread.currentThread().getId())
                    .append("] delayed anonymization of consumer ")
                    .append(result == null ? "(null)" : Long.toString(result.id, 10))
                    .append(" (partner ")
                    .append(partnerId)
                    .append(", username ")
                    .append(username)
                    .append("): immediate anonymization of any previous records: ")
                    .append((anonTime - startTime) / 1000000L)
                    .append(" ms, marking for delayed anonymization of current record: ")
                    .append((deleteTime - anonTime) / 1000000L)
                    .append(" ms")
                    .toString());
        }
        else { // immediate anonymization TODO: fix parent_id is NULL bug, see delayed anonymization above
            result = internalGetByPartnerIdAndUsernameAndTypeAndParent(partnerId, username, consumerTypeId, parentConsumerId, true);
            try (final PreparedStatement st = dataLink.prepareStatement("update consumer set username = null, partner_id = null, status = 3, deleted = coalesce(deleted, current_timestamp at time zone 'utc') where partner_id = ? and username = ? and type_id = ? and parent_id = ? returning id, deleted")) {
                st.setLong(1, partnerId);
                st.setString(2, username);
                st.setLong(3, consumerTypeId);
                if (parentConsumerId == 0L) st.setNull(4, Types.BIGINT);
                else st.setLong(4, parentConsumerId);
                try (final ResultSet rs = st.executeQuery()) {
                    while (rs.next()) {
                        final long consumerId = rs.getLong(1);
                        Timestamp deleted = rs.getTimestamp(2, utcCalendar);
                        if (rs.wasNull()) deleted = null;
                        // do a cascade deletion of children
                        deleteAllChildren(consumerId, deleted);
                        if ((result != null) && (consumerId == result.id)) {
                            result = new ConsumerEntity(consumerId, null, -1L, result.activated, deleted == null ? -1L : deleted.getTime(), (short) 0, result.relations, consumerTypeId, parentConsumerId);
                        }
                    }
                }
            }
            catch (SQLException e) {
                throw new DatabaseException("Failed to anonymize old records for consumer \"" + username.replace("\"", "\\\"") + "\" and partner ID " + partnerId + ": " + e.toString(), e);
            }
            final long deleteTime = System.nanoTime();

            log.debug(new StringBuilder(200)
                    .append("[")
                    .append(Thread.currentThread().getId())
                    .append("] immediate anonymization of consumer ")
                    .append(result == null ? "(null)" : Long.toString(result.id, 10))
                    .append(" (partner ")
                    .append(partnerId)
                    .append(", username ")
                    .append(username)
                    .append("): ")
                    .append((deleteTime - startTime) / 1000000L)
                    .append(" ms")
                    .toString());

            // status=3 records are picked up by a cron job (or such) which then anonymizes their events and flips status to 0
        }

        return result;
    }

    void anonymize(final long consumerId) {
        try (final PreparedStatement st = dataLink.prepareStatement("update consumer set username = null, partner_id = null, status = 3, deleted = coalesce(deleted, current_timestamp at time zone 'utc') where id = ?")) {
            st.setLong(1, consumerId);
            st.executeUpdate();
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to anonymize consumer with ID " + consumerId + ": " + e.toString(), e);
        }

        // status=3 records are picked up by a cron job (or such) which then anonymizes their events and flips status to 0
    }

    void anonymizeNow(final long partnerId, final String username, final long consumerTypeId, final long parentConsumerId) {
        // retrieve all inactive records belonging to the consumer and anonymize right away
        // TODO: hook into timer for records with status = 2, and cancel those timers
        try (final PreparedStatement st = dataLink.prepareStatement("update consumer set username = null, partner_id = null, status = case status when 0 then 0 else 3 end, deleted = coalesce(deleted, current_timestamp at time zone 'utc') where status in (0, 2) and partner_id = ? and username = ? and type_id = ? and parent_id = ?")) {
            st.setLong(1, partnerId);
            st.setString(2, username);
            st.setLong(3, consumerTypeId);
            if (parentConsumerId == 0L) st.setNull(4, Types.BIGINT);
            else st.setLong(4, parentConsumerId);
            st.executeUpdate();
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to anonymize old records for consumer \"" + username.replace("\"", "\\\"") + "\" and partner ID " + partnerId + ": " + e.toString(), e);
        }
        // status=3 records are picked up by a cron job (or such) which then anonymizes their events and flips status to 0
    }

    private static final String SELECT_RELATIONS = "select id, product_id, relation_type_id, relation_start, relation_end from relation_consumer_product where consumer_id = ?";
    private static final String SELECT_RELATIONS_FOR_UPDATE = SELECT_RELATIONS + " for update";

    private List<RelationConsumerProductEntity> getRelations(final long consumerId, final boolean forUpdate) {
        final Calendar cal = utcCalendar;
        try (final PreparedStatement st = dataLink.prepareStatement(forUpdate ? SELECT_RELATIONS_FOR_UPDATE : SELECT_RELATIONS)) {
            st.setLong(1, consumerId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return Collections.emptyList();
                final List<RelationConsumerProductEntity> result = new ArrayList<>();
                Timestamp relationStart = rs.getTimestamp(4, cal);
                if (rs.wasNull()) relationStart = null;
                Timestamp relationEnd = rs.getTimestamp(5, cal);
                if (rs.wasNull()) relationEnd = null;
                result.add(new RelationConsumerProductEntity(rs.getLong(1), consumerId, rs.getLong(2), rs.getLong(3), relationStart == null ? -1L : relationStart.getTime(), relationEnd == null ? -1L : relationEnd.getTime()));
                while (rs.next()) {
                    relationStart = rs.getTimestamp(4, cal);
                    if (rs.wasNull()) relationStart = null;
                    relationEnd = rs.getTimestamp(5, cal);
                    if (rs.wasNull()) relationEnd = null;
                    result.add(new RelationConsumerProductEntity(rs.getLong(1), consumerId, rs.getLong(2), rs.getLong(3), relationStart == null ? -1L : relationStart.getTime(), relationEnd == null ? -1L : relationEnd.getTime()));
                }
                return result;
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to retrieve relations for consumer with ID " + consumerId + ": " + e.toString(), e);
        }
    }

    private static final String SELECT_ACTIVE_CHILD_CONSUMERS = "select id, username, partner_id, activated, deleted, type_id from consumer where status = 1 and parent_id = ?";
    private static final String SELECT_ACTIVE_CHILD_CONSUMERS_FOR_UPDATE = SELECT_ACTIVE_CHILD_CONSUMERS + " for update";

    @Override
    public List<ConsumerEntity> getActiveChildren(final long consumerId, final boolean forUpdate) {
        final Calendar cal = utcCalendar;
        try (final PreparedStatement st = dataLink.prepareStatement(forUpdate ? SELECT_ACTIVE_CHILD_CONSUMERS_FOR_UPDATE : SELECT_ACTIVE_CHILD_CONSUMERS)) {
            st.setLong(1, consumerId);
            try (final ResultSet rs = st.executeQuery()) {
                if (!rs.next()) return Collections.emptyList();
                final List<ConsumerEntity> result = new ArrayList<>();
                do {
                    final long id = rs.getLong(1);
                    String username = rs.getString(2);
                    if (rs.wasNull()) username = null;
                    long partnerId = rs.getLong(3);
                    if (rs.wasNull()) partnerId = 0L;
                    Timestamp activated = rs.getTimestamp(4, cal);
                    if (rs.wasNull()) activated = null;
                    Timestamp deleted = rs.getTimestamp(5, cal);
                    if (rs.wasNull()) deleted = null;
                    result.add(new ConsumerEntity(id, username, partnerId, activated == null ? -1L : activated.getTime(), deleted == null ? -1L : deleted.getTime(), (short) 1, getRelations(consumerId, false), rs.getLong(6), consumerId));
                } while (rs.next());
                return result;
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to retrieve children of consumer with ID " + consumerId + ": " + e.toString(), e);
        }
    }

    private ImmutableMap<String, Long> mapChildren(final List<ConsumerEntity> children) {
        if ((children == null) || children.isEmpty()) return emptyChildren;
        final ImmutableMap.Builder<String, Long> resultBuilder = ImmutableMap.builder();
        for (final ConsumerEntity consumer : children) {
            resultBuilder.put(consumer.username == null ? "" : consumer.username, consumer.id);
        }
        return resultBuilder.build();
    }

    private Map<RelationKey, List<RelationConsumerProductEntity>> getMappedRelations(final long consumerId, final boolean forUpdate) {
        final Calendar cal = utcCalendar;
        final Map<RelationKey, List<RelationConsumerProductEntity>> map = new HashMap<>();
        try (final PreparedStatement st = dataLink.prepareStatement(forUpdate ? SELECT_RELATIONS_FOR_UPDATE : SELECT_RELATIONS)) {
            st.setLong(1, consumerId);
            try (final ResultSet rs = st.executeQuery()) {
                listProcessing:
                while (rs.next()) {
                    Timestamp startTimestamp = rs.getTimestamp(4, cal);
                    final long relationStart = rs.wasNull() || (startTimestamp == null) ? -1L : startTimestamp.getTime();
                    Timestamp endTimestamp = rs.getTimestamp(5, cal);
                    final long relationEnd = rs.wasNull() || (endTimestamp == null) ? -1L : endTimestamp.getTime();
                    final RelationConsumerProductEntity relation = new RelationConsumerProductEntity(rs.getLong(1), consumerId, rs.getLong(2), rs.getLong(3), relationStart, relationEnd);
                    final RelationKey key = new RelationKey(relation.productId, relation.relationTypeId);
                    final List<RelationConsumerProductEntity> relationList = map.get(key);
                    if (relationList == null) {
                        final List<RelationConsumerProductEntity> newRelationList = new ArrayList<>();
                        map.put(key, newRelationList);
                        newRelationList.add(relation);
                        continue;
                    }
                    // sorted insertion into an existing list
                    final int listSize = relationList.size();
                    int i = 0;
                    RelationConsumerProductEntity r = relationList.get(0); // it is guaranteed that if a list exists, it is not empty
                    // scan until the first relation with the same or bigger start-time is found
                    while (r.relationStart < relationStart) {
                        i++;
                        if (i >= listSize) {
                            // no more items in the list, append and continue with the next relation
                            relationList.add(relation);
                            continue listProcessing;
                        }
                        r = relationList.get(i);
                    }
                    // scan until the first relation with the same start-time and bigger end-time
                    while ((r.relationStart == relationStart) && (r.relationEnd <= relationEnd)) {
                        i++;
                        if (i >= listSize) {
                            // no more items in the list, append and continue with the next relation
                            relationList.add(relation);
                            continue listProcessing;
                        }
                        r = relationList.get(i);
                    }
                    // we have the first relation with the same start-time and a bigger end-time, or with a bigger start-time, insert before it
                    relationList.add(i, relation);
                }
            }
        }
        catch (SQLException e) {
            throw new DatabaseException("Failed to retrieve relations for consumer with ID " + consumerId + ": " + e.toString(), e);
        }
        return map;
    }

    private static class FillHelper {
        final List<RelationConsumerProductEntity> relations;
        final long id;
        final String username;
        final long partnerId;
        final long activated;
        final long deleted;
        final short status;
        final long consumerTypeId;
        final long parentConsumerId;

        FillHelper(final long id, final String username, final long partnerId, final long activated, final long deleted, final short status, final long consumerTypeId, final long parentConsumerId) {
            this.id = id;
            this.username = username;
            this.partnerId = partnerId;
            this.activated = activated;
            this.deleted = deleted;
            this.status = status;
            relations = new ArrayList<>();
            this.consumerTypeId = consumerTypeId;
            this.parentConsumerId = parentConsumerId;
        }

        public ConsumerEntity asConsumer() {
            return new ConsumerEntity(id, username, partnerId, activated, deleted, status, relations, consumerTypeId,  parentConsumerId);
        }

        public ConsumerEntity asConsumer(final List<RelationConsumerProductEntity> newRelations) {
            return new ConsumerEntity(id, username, partnerId, activated, deleted, status, newRelations, consumerTypeId, parentConsumerId);
        }
    }

    private static class SelectOrCreateInfo {
        final long consumerId;
        final long activated;
        final long deleted;
        final boolean isNew;

        SelectOrCreateInfo(final long consumerId, final long activated, final long deleted, final boolean isNew) {
            this.consumerId = consumerId;
            this.activated = activated;
            this.deleted = deleted;
            this.isNew = isNew;
        }
    }

    private static class RelationKey {
        final long productId;
        final long relationTypeId;
        private final int hash;
        RelationKey(final long productId, final long relationTypeId) {
            this.productId = productId;
            this.relationTypeId = relationTypeId;
            int hash = 17;
            hash = (31 * hash) + (int)(relationTypeId ^ (relationTypeId >>> 32));
            hash = (31 * hash) + (int)(productId ^ (productId >>> 32));
            this.hash = hash;
        }
        @Override
        public boolean equals(final Object obj) {
            if (obj == null) return false;
            if (obj instanceof RelationKey) {
                final RelationKey other = (RelationKey) obj;
                if (other.productId != this.productId) return false;
                return other.relationTypeId == this.relationTypeId;
            }
            return false;
        }
        @Override
        public int hashCode() {
            return hash;
        }
    }
}
