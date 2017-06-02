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
import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.data.AttributeCodes;
import com.gurucue.recommendations.data.CloseListener;
import com.gurucue.recommendations.data.ConsumerEventTypeCodes;
import com.gurucue.recommendations.data.ConsumerListener;
import com.gurucue.recommendations.data.DataLink;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.DataTypeCodes;
import com.gurucue.recommendations.data.LanguageCodes;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.dto.ConsumerEntity;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;
import com.gurucue.recommendations.entity.ConsumerEvent;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.json.JsonObject;
import com.gurucue.recommendations.queueing.BoundedQueueFixedPoolProcessing;
import com.gurucue.recommendations.queueing.Processor;
import com.gurucue.recommendations.queueing.ProcessorFactory;
import com.gurucue.recommendations.queueing.QueueProcessor;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Data provider implementation for PostgreSQL.
 */
public final class PostgreSqlDataProvider implements DataProvider, JdbcDataProvider {
    private static final String JDBC_DRIVER = "org.postgresql.Driver";
    private static final String ENV_URL = "PROVIDER_POSTGRESQL_URL";
    private static final String ENV_USERNAME = "PROVIDER_POSTGRESQL_USERNAME";
    private static final String ENV_PASSWORD = "PROVIDER_POSTGRESQL_PASSWORD";
    private static final String ENV_CONSUMER_EVENT_WRITERS = "PROVIDER_POSTGRESQL_CONSUMER_EVENT_WRITERS";
    private static final String ENV_CONSUMER_EVENT_QUEUE = "PROVIDER_POSTGRESQL_CONSUMER_EVENT_QUEUE";

    private static final Logger log = LogManager.getLogger(PostgreSqlDataProvider.class);

    // makes sure the PostgreSQL JDBC driver is loaded
    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (final ClassNotFoundException e) {
            throw new DatabaseException("Unable to load the JDBC driver for PostgreSQL: " + e.toString(), e);
        }
    }

    /**
     * Factory method for creation of a PostgreSQL data provider instance
     * which is configured from environment variables. The user should set
     * environment variables <code>PROVIDER_POSTGRESQL_URL</code>,
     * <code>PROVIDER_POSTGRESQL_USERNAME</code> and
     * <code>PROVIDER_POSTGRESQL_PASSWORD</code> before calling this method.
     *
     * @return a new PostgreSQL data provider instance
     */
    public static PostgreSqlDataProvider create() {
        int consumerEventWriters;
        int consumerEventQueueSize;
        try {
            consumerEventWriters = Integer.parseInt(System.getenv(ENV_CONSUMER_EVENT_WRITERS), 10);
        }
        catch (NumberFormatException e) {
            consumerEventWriters = 5;
        }
        try {
            consumerEventQueueSize = Integer.parseInt(System.getenv(ENV_CONSUMER_EVENT_QUEUE), 10);
        }
        catch (NumberFormatException e) {
            consumerEventQueueSize = 100000;
        }
        return new PostgreSqlDataProvider(System.getenv(ENV_URL), System.getenv(ENV_USERNAME), System.getenv(ENV_PASSWORD), consumerEventWriters, consumerEventQueueSize);
    }

    /**
     * Factory method for creation of a PostgreSQL data provider instance
     * which is configured with the given arguments.
     *
     * @param dbUrl the JDBC URL of the database to connect to
     * @param dbUsername username with which to login to the database
     * @param dbPassword password to use to login to the database
     * @param consumerEventWriters the initial number of threads processing the consumer event queue
     * @param consumerEventQueueSize the initial size of the consumer event queue
     * @return a new PostgreSQL data provider instance
     */
    public static PostgreSqlDataProvider create(final String dbUrl, final String dbUsername, final String dbPassword, final int consumerEventWriters, final int consumerEventQueueSize) {
        return new PostgreSqlDataProvider(dbUrl, dbUsername, dbPassword, consumerEventWriters, consumerEventQueueSize);
    }

    public static PostgreSqlDataProvider create(final String dbUrl, final String dbUsername, final String dbPassword) {
        return new PostgreSqlDataProvider(dbUrl, dbUsername, dbPassword, 5, 100000);
    }

    // basic stuff

    final String jdbcUrl;
    final String jdbcUsername;
    final String jdbcPassword;

    private final ReadWriteLock rwLock;
    private final Lock readLock;
    private final Lock writeLock;
    private boolean isClosed;
    private final Queue<PostgreSqlDataLink> linkQueue;
    private final AtomicInteger linkQueueSize;
    private final ExecutorService asyncJobExecutor;
    private final ConcurrentMap<PostgreSqlDataLink, Boolean> activeLinks;

    // caches

    final CacheAttribute attributeCache;
    final CacheDataType dataTypeCache;
    final CacheLanguage languageCache;
    final CachePartner partnerCache;
    final CacheProductType productTypeCache;
    final CacheRecommender recommenderCache;
    final CachePartnerRecommender partnerRecommenderCache;
    final CacheRecommenderConsumerOverride recommenderConsumerOverrideCache;
    final CacheConsumerEventType consumerEventTypeCache;
    final CacheRelationType relationTypeCache;

    // misc

    final Partner PARTNER_ZERO;
    private final List<CloseListener> closeListeners = new ArrayList<>();
    private final Set<ConsumerListener> consumerListeners = new HashSet<>(); // synchronize on the set instance before accessing it
    final AttributeCodes attributeCodes;
    final ProductTypeCodes productTypeCodes;
    final ConsumerEventTypeCodes consumerEventTypeCodes;
    final DataTypeCodes dataTypeCodes;
    final LanguageCodes languageCodes;
    final String sqlSelectProductsOverlappingInterval;

    final BoundedQueueFixedPoolProcessing<ConsumerEventPayload> consumerEventQueue;
    final QueueProcessor<Long> consumerAnonymizer;

    // implementation

    private PostgreSqlDataProvider(final String dbUrl, final String dbUsername, final String dbPassword, final int consumerEventWorkers, final int consumerEventQueueSize) {
        this.jdbcUrl = dbUrl;
        this.jdbcUsername = dbUsername;
        this.jdbcPassword = dbPassword;
        if ((jdbcUrl == null) || (jdbcUrl.length() == 0)) throw new DatabaseException("PostgreSQL data provider: no database URL provided, and none set in the environment variable " + ENV_URL);
        if ((jdbcUsername == null) || (jdbcUsername.length() == 0)) throw new DatabaseException("PostgreSQL data provider: no database username provided, and none set in the environment variable " + ENV_USERNAME);
        if ((jdbcPassword == null) || (jdbcPassword.length() == 0)) throw new DatabaseException("PostgreSQL data provider: no database login password provided, and none set in the environment variable " + ENV_PASSWORD);
        log.info("Using driver " + JDBC_DRIVER + " for database at " + jdbcUrl + ", username=" + jdbcUsername + ", password=" + jdbcPassword);

        rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
        isClosed = false;
        linkQueue = new ConcurrentLinkedQueue<>();
        linkQueueSize = new AtomicInteger(0);
        asyncJobExecutor = Executors.newFixedThreadPool(4);
        activeLinks = new ConcurrentHashMap<>(500);

        readLock.lock();
        try {
            attributeCache = new CacheAttribute(this);
            dataTypeCache = new CacheDataType(this);
            languageCache = new CacheLanguage(this);
            partnerCache = new CachePartner(this);
            productTypeCache = new CacheProductType(this);
            recommenderCache = new CacheRecommender(this);
            partnerRecommenderCache = new CachePartnerRecommender(this);
            recommenderConsumerOverrideCache = new CacheRecommenderConsumerOverride(this);
            consumerEventTypeCache = new CacheConsumerEventType(this);
            relationTypeCache = new CacheRelationType(this);
            clearCaches();
            PARTNER_ZERO = partnerCache.byId(Partner.PARTNER_ZERO_ID);
            if (PARTNER_ZERO == null) throw new DatabaseException("Partner zero (ID " + Partner.PARTNER_ZERO_ID + ") not defined");

            try (final JdbcDataLink link = newJdbcDataLink()) {
                attributeCodes = new AttributeCodes(link.getAttributeManager());
                productTypeCodes = new ProductTypeCodes(link.getProductTypeManager());
                consumerEventTypeCodes = new ConsumerEventTypeCodes(link.getConsumerEventTypeManager());
                dataTypeCodes = new DataTypeCodes(link.getDataTypeManager());
                languageCodes = new LanguageCodes(link.getLanguageManager());
            }

            StringBuilder sb;

            // define statement pools whose definitions depend on caches being available
/* raw SQL:
select p.id, pr.id, pr.partner_product_code
from product p
inner join product_attribute pc on pc.product_id = p.id and pc.attribute_id = 52
inner join product_attribute pb on pb.product_id = p.id and pb.attribute_id = 56
inner join product_attribute pe on pe.product_id = p.id and pe.attribute_id = 57
left join product pr on pr.product_id = p.id
where p.product_type_id = 3
and p.partner_id = 0
--and p.id != 123
and pc.original_value = 'CHANNEL42'
and (
  (cast(pb.original_value as bigint) >= 1387346400 and cast(pb.original_value as bigint) < 1387347840) or -- begins within the interval
  (cast(pe.original_value as bigint) <= 1387347840 and cast(pe.original_value as bigint) > 1387346400) or -- ends within the interval
  (cast(pb.original_value as bigint) <= 1387346400 and cast(pe.original_value as bigint) >= 1387347840) -- begins before and ends after the interval
)
 */
            sb = new StringBuilder();
            sb.append("select p.id, pr.id, pr.partner_product_code from product p inner join product_attribute pc on pc.product_id = p.id and pc.attribute_id = ");
            sb.append(attributeCodes.idForTvChannel);
            sb.append(" inner join product_attribute pb on pb.product_id = p.id and pb.attribute_id = ");
            sb.append(attributeCodes.idForBeginTime);
            sb.append(" inner join product_attribute pe on pe.product_id = p.id and pe.attribute_id = ");
            sb.append(attributeCodes.idForEndTime);
            sb.append(" left join product pr on pr.product_id = p.id");
            sb.append(" where p.partner_id = ");
            sb.append(PARTNER_ZERO.getId());
            sb.append(" and p.product_type_id = ");
            sb.append(productTypeCodes.idForTvProgramme);
            sb.append(" and p.deleted is null");
            sb.append(" and pc.original_value = ? and ("); // parameter #1: tv-channel-id
            sb.append(" (cast(pb.original_value as bigint) >= ? and cast(pb.original_value as bigint) < ?) or"); // parameter #2: begin-time, parameter #3: end-time
            sb.append(" (cast(pe.original_value as bigint) <= ? and cast(pe.original_value as bigint) > ?) or"); // parameter #4: end-time, parameter #5: begin-time
            sb.append(" (cast(pb.original_value as bigint) <= ? and cast(pe.original_value as bigint) >= ?))");  // parameter #6: begin-time, parameter #7: end-time
            sqlSelectProductsOverlappingInterval = sb.toString();

            consumerEventQueue = new BoundedQueueFixedPoolProcessing<>("Consumer event", consumerEventWorkers, consumerEventQueueSize, new ProcessorFactory<ConsumerEventPayload>() {
                @Override
                public Processor<ConsumerEventPayload> createProcessor() {
                    return new ConsumerEventProcessor(PostgreSqlDataProvider.this);
                }
            });

            consumerAnonymizer = new QueueProcessor<>("Consumer anonymizer", new ConsumerAnonymizerFactory(this), 1);
            consumerAnonymizer.start();

            // now sync the delayed consumer anonymizations
            final long now = Timer.currentTimeMillis() + 60000L; // a minute ahead plays no role
            final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            try (final JdbcDataLink link = newJdbcDataLink()) {
                try (final PreparedStatement st = link.prepareStatement("select id, deleted from consumer where status = 2", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    st.setFetchSize(5000);
                    try (final ResultSet rs = st.executeQuery()) {
                        while (rs.next()) {
                            final long consumerId = rs.getLong(1);
                            final Timestamp t = rs.getTimestamp(2, utcCalendar);
                            final long deleted = (rs.wasNull() || (t == null) ? Timer.currentTimeMillis() : t.getTime()) + 86400000L;
                            if (deleted <= now) consumerAnonymizer.submit(consumerId);
                            else {
                                Timer.INSTANCE.schedule(deleted, new ConsumerAnonymization(this, consumerId, deleted));
                            }
                        }
                    }
                }
                catch (SQLException e) {
                    final String cause = "Failed to fetch a list of consumers that were deleted but not yet anonymized: " + e.toString();
                    log.error(cause, e);
                    throw new DatabaseException(cause, e);
                }
            }

            // TODO: on exception in constructor stop any threads
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public DataLink newDataLink() {
        return newJdbcDataLink();
    }

    @Override
    public void clearCaches() {
        readLock.lock();
        try {
            if (isClosed) throw new DatabaseException("The PostgreSqlDataProvider is closed.");
            attributeCache.refresh();
            dataTypeCache.refresh();
            languageCache.refresh();
            partnerCache.refresh();
            productTypeCache.refresh();
            recommenderCache.refresh();
            partnerRecommenderCache.refresh();
            recommenderConsumerOverrideCache.refresh();
            consumerEventTypeCache.refresh();
            relationTypeCache.refresh();
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public void close() {
        // this lock serializes close() and sets isClosed
        writeLock.lock();
        try {
            if (isClosed) return;
            isClosed = true;
        }
        finally {
            writeLock.unlock();
        }
        // do the following without holding a lock, otherwise deadlocks are possible
        log.info("Stopping consumer event queue...");
        consumerEventQueue.shutdown();
        log.info("Stopping consumer anonymizer...");
        consumerAnonymizer.stop();
        log.info("Stopping async executor...");
        asyncJobExecutor.shutdownNow();
        try {
            asyncJobExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for asynchronous job executor to terminate: " + e.toString());
        }
        log.info("Closing connections...");
        for (final PostgreSqlDataLink link : activeLinks.keySet()) {
            try {
                link.connection.close();
            }
            catch (SQLException e) {
                log.error("Failed to close a JDBC connection while the provider is closing: " + e.toString(), e);
            }
            catch (RuntimeException e) {
                log.error("Failed to close a JDBC connection while the provider is closing: " + e.toString(), e);
            }
        }
        activeLinks.clear();
        final List<CloseListener> listeners;
        synchronized (closeListeners) {
            listeners = new ArrayList(closeListeners);
        }
        log.info("Invoking onClose listeners...");
        for (int i = listeners.size() - 1; i >= 0; i--) {
            // invoke in reverse registration order
            try {
                listeners.get(i).onClose(this);
            }
            catch (RuntimeException e) {
                log.error("Exception while invoking a CloseListener: " + e.toString(), e);
            }
        }
        log.info("PostgreSQL provider is closed.");
    }

    @Override
    public void registerOnClose(final CloseListener closeListener) {
        synchronized (closeListeners) {
            if (closeListeners.contains(closeListener)) return;
            closeListeners.add(closeListener);
        }
    }

    @Override
    public void runAsync(final Runnable job) {
        asyncJobExecutor.execute(job);
    }

    @Override
    public AttributeCodes getAttributeCodes() {
        return attributeCodes;
    }

    @Override
    public ProductTypeCodes getProductTypeCodes() {
        return productTypeCodes;
    }

    @Override
    public ConsumerEventTypeCodes getConsumerEventTypeCodes() {
        return consumerEventTypeCodes;
    }

    @Override
    public DataTypeCodes getDataTypeCodes() {
        return dataTypeCodes;
    }

    @Override
    public LanguageCodes getLanguageCodes() {
        return languageCodes;
    }

    @Override
    public void queueConsumerEvent(final ConsumerEvent consumerEvent) throws InterruptedException {
        consumerEventQueue.submit(new ConsumerEventPayload(consumerEvent));
    }

    @Override
    public void resizeConsumerEventQueueSize(final int newSize) {
        consumerEventQueue.resizeQueue(newSize);
    }

    @Override
    public void resizeConsumerEventQueueThreadPool(final int newSize) {
        consumerEventQueue.resizeThreadPoolSize(newSize);
    }

    @Override
    public void registerConsumerListener(final ConsumerListener listener) {
        synchronized (consumerListeners) {
            consumerListeners.add(listener);
        }
    }

    @Override
    public void unregisterConsumerListener(final ConsumerListener listener) {
        synchronized (consumerListeners) {
            consumerListeners.remove(listener);
        }
    }

    @Override
    public PostgreSqlDataLink newJdbcDataLink() {
        readLock.lock();
        try {
            if (isClosed) throw new DatabaseException("The PostgreSqlDataProvider is closed.");
            for (;;) {
                final PostgreSqlDataLink link = linkQueue.poll();
                if (link == null) return createLink();
                linkQueueSize.decrementAndGet();
                if (link.isValid()) return link;
            }
        }
        finally {
            readLock.unlock();
        }
    }


    /**
     * Invoked by data links when they are closed. If a link is still valid
     * (a commit issued in a background thread goes through without an error),
     * then it is cached to be recycled at a future {@link #newDataLink()} or
     * {@link #newJdbcDataLink()}. Otherwise the link is discarded.
     *
     * @param link the link being closed
     */
    void dataLinkClosed(final PostgreSqlDataLink link) {
        readLock.lock();
        try {
            if (isClosed) return; // if not already, link will be destroyed eventually
            runAsync(new LinkQueueSubmitter(link));
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Invoked by data links when they are destroyed (discarded). The provider
     * maintains a list of all created links so it can do a cleanup when it is
     * closed. When a link is destroyed, it is removed from the list by calling
     * this method.
     *
     * @param link the link being destroyed
     */
    void dataLinkDestroyed(final PostgreSqlDataLink link) {
        readLock.lock();
        try {
            if (isClosed) return; // if not already removed from the activeLinks, it will be eventually
            activeLinks.remove(link);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Internal method to instantiate a data link and put it into the internal
     * list of active data links.
     *
     * @return a new instance of a data link
     */
    private PostgreSqlDataLink createLink() {
        final PostgreSqlDataLink link = new PostgreSqlDataLink(this);
        activeLinks.put(link, Boolean.TRUE);
        return link;
    }

    private final TLongObjectMap<ConsumerAnonymization> delayedAnonymizations = new TLongObjectHashMap<>();

    void setDelayedConsumerAnonymization(final ConsumerEntity consumer) {
        ConsumerAnonymization anonymization = new ConsumerAnonymization(this, consumer.id, (consumer.deleted < 0L ? Timer.currentTimeMillis() : consumer.deleted) + 86400000L);
        Timer.INSTANCE.schedule(anonymization.scheduledTimeMillis, anonymization);
        final ConsumerAnonymization previousAnonymization;
        synchronized (delayedAnonymizations) {
            previousAnonymization = delayedAnonymizations.put(consumer.id, anonymization);
        }
        if (previousAnonymization != null) Timer.INSTANCE.unschedule(previousAnonymization.scheduledTimeMillis, previousAnonymization);
    }

    void cancelDelayedConsumerAnonymization(final PostgreSqlDataLink link, final long consumerId) {
        final ConsumerAnonymization anonymization;
        synchronized (delayedAnonymizations) {
            anonymization = delayedAnonymizations.get(consumerId);
        }
        if (anonymization == null) return;
        link.addCommitJob(new Runnable() {
            @Override
            public void run() {
                synchronized (delayedAnonymizations) {
                    final ConsumerAnonymization previousAnonymization = delayedAnonymizations.remove(consumerId);
                    if (previousAnonymization != anonymization) delayedAnonymizations.put(consumerId, previousAnonymization); // cancel if not the same
                }
                Timer.INSTANCE.unschedule(anonymization.scheduledTimeMillis, anonymization);
            }
        });
    }

    public static PGobject toJsonPGobject(final JsonObject obj) throws SQLException {
        final PGobject jsonObj = new PGobject();
        jsonObj.setType("jsonb");
        jsonObj.setValue(obj.serialize());
        return jsonObj;
    }

    /**
     * A <code>Runnable</code> implementing background queueing of a closed
     * data link. If the "recycle queue" is already too long, then it destroys
     * the link, otherwise it commits it and queues it.
     */
    private final class LinkQueueSubmitter implements Runnable {
        final PostgreSqlDataLink link;

        LinkQueueSubmitter(final PostgreSqlDataLink link) {
            this.link = link;
        }

        @Override
        public void run() {
            try {
                if (linkQueueSize.incrementAndGet() >= 30) {
                    linkQueueSize.decrementAndGet();
                    link.destroy();
                } else {
                    try {
                        link.commit();
                    } catch (DatabaseException e) {
                        linkQueueSize.decrementAndGet();
                        link.destroy();
                        return;
                    }
                    linkQueue.add(link);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception when returning a database connection to the pool: " + e.toString(), e);
            }
        }
    }
}
