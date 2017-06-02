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
import com.gurucue.recommendations.data.DataLink;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.entitymanager.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>DataLink</code> implementation for PostgreSQL.
 * THIS CLASS IS NOT THREAD-SAFE!
 */
public final class PostgreSqlDataLink implements DataLink, JdbcDataLink {
    private static final Logger log = LogManager.getLogger(PostgreSqlDataLink.class);

    final PostgreSqlDataProvider provider;
    final Connection connection;
    final List<Runnable> commitJobs = new ArrayList<>();

    private static final ProductManagerImpl productManager = new ProductManagerImpl();
    private PartnerManagerImpl partnerManager = null;
    private RecommenderManagerImpl recommenderManager = null;
    private ConsumerManagerImpl consumerManager = null;
    private ProductTypeManagerImpl productTypeManager = null;
    private PartnerRecommenderManagerImpl partnerRecommenderManager = null;
    private RecommenderConsumerOverrideManagerImpl recommenderConsumerOverrideManager = null;
    private ConsumerEventManagerImpl consumerEventManager = null;
    private ConsumerEventTypeManagerImpl consumerEventTypeManager = null;
    private RelationTypeManagerImpl relationTypeManager = null;
    private LogSvcRecommendationManagerImpl logSvcRecommendationManager = null;
    private DataTypeManagerImpl dataTypeManager = null;
    private LanguageManagerImpl languageManager = null;
    private AttributeManagerImpl attributeManager = null;
    private ProductTypeAttributeManagerImpl productTypeAttributeManager = null;
    private LogSvcSearchManagerImpl logSvcSearchManager = null;

    PostgreSqlDataLink(final PostgreSqlDataProvider provider) {
        this.provider = provider;
        try {
            connection = DriverManager.getConnection(provider.jdbcUrl, provider.jdbcUsername, provider.jdbcPassword);
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        } catch (final SQLException e) {
            final String reason = "Failed to open a connection to the database: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public void close() {
        commitJobs.clear();
        provider.dataLinkClosed(this);
    }

    void destroy() {
        try {
            connection.close();
        } catch (SQLException e) {
            // Do not treat exceptions at connection closing time as fatal. Just log them.
            log.error("destroy(): A connection failed to close(): " + e.toString(), e);
        } finally {
            provider.dataLinkDestroyed(this);
        }
    }

    @Override
    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            final String reason = "Failed to commit: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
        for (final Runnable job : commitJobs) {
            try {
                job.run();
            }
            catch (Throwable e) {
                log.error("A commit job failed, after the database transaction was committed: " + e.toString(), e);
            }
        }
        commitJobs.clear();
    }

    public void addCommitJob(final Runnable job) {
        commitJobs.add(job);
    }

    @Override
    public void rollback() {
        commitJobs.clear();
        try {
            connection.rollback();
        } catch (SQLException e) {
            final String reason = "Failed to rollback: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public DataProvider getProvider() {
        return provider;
    }

    @Override
    public Partner getPartnerZero() {
        return provider.PARTNER_ZERO;
    }

    @Override
    public AttributeManager getAttributeManager() {
        if (attributeManager == null) attributeManager = new AttributeManagerImpl(this);
        return attributeManager;
    }

    @Override
    public ConsumerEventManager getConsumerEventManager() {
        if (consumerEventManager == null) consumerEventManager = new ConsumerEventManagerImpl(this);
        return consumerEventManager;
    }

    @Override
    public ConsumerEventTypeManager getConsumerEventTypeManager() {
        if (consumerEventTypeManager == null) consumerEventTypeManager = new ConsumerEventTypeManagerImpl(this);
        return consumerEventTypeManager;
    }

    @Override
    public ConsumerManagerImpl getConsumerManager() {
        if (consumerManager == null) consumerManager = new ConsumerManagerImpl(this);
        return consumerManager;
    }

    @Override
    public DataTypeManager getDataTypeManager() {
        if (dataTypeManager == null) dataTypeManager = new DataTypeManagerImpl(this);
        return dataTypeManager;
    }

    @Override
    public LanguageManager getLanguageManager() {
        if (languageManager == null) languageManager = new LanguageManagerImpl(this);
        return languageManager;
    }

    @Override
    public LogSvcRecommendationManager getLogSvcRecommendationManager() {
        if (logSvcRecommendationManager == null) logSvcRecommendationManager = new LogSvcRecommendationManagerImpl(this);
        return logSvcRecommendationManager;
    }

    @Override
    public PartnerManager getPartnerManager() {
        if (partnerManager == null) partnerManager = new PartnerManagerImpl(this);
        return partnerManager;
    }

    @Override
    public PartnerRecommenderManager getPartnerRecommenderManager() {
        if (partnerRecommenderManager == null) partnerRecommenderManager = new PartnerRecommenderManagerImpl(this);
        return partnerRecommenderManager;
    }

    @Override
    public ProductManager getProductManager() {
        return productManager;
    }

    @Override
    public ProductTypeManager getProductTypeManager() {
        if (productTypeManager == null) productTypeManager = new ProductTypeManagerImpl(this);
        return productTypeManager;
    }

    @Override
    public ProductTypeAttributeManager getProductTypeAttributeManager() {
        if (productTypeAttributeManager == null) productTypeAttributeManager = new ProductTypeAttributeManagerImpl(this);
        return productTypeAttributeManager;
    }

    @Override
    public RecommenderConsumerOverrideManager getRecommenderConsumerOverrideManager() {
        if (recommenderConsumerOverrideManager == null) recommenderConsumerOverrideManager = new RecommenderConsumerOverrideManagerImpl(this);
        return recommenderConsumerOverrideManager;
    }

    @Override
    public RecommenderManager getRecommenderManager() {
        if (recommenderManager == null) recommenderManager = new RecommenderManagerImpl(this);
        return recommenderManager;
    }

    @Override
    public RelationTypeManager getRelationTypeManager() {
        if (relationTypeManager == null) relationTypeManager = new RelationTypeManagerImpl(this);
        return relationTypeManager;
    }

    @Override
    public LogSvcSearchManager getLogSvcSearchManager() {
        if (logSvcSearchManager == null) logSvcSearchManager = new LogSvcSearchManagerImpl(this);
        return logSvcSearchManager;
    }

    @Override
    public boolean isValid() {
        try {
            if (connection.isClosed()) return false;
            try (final Statement test = connection.createStatement()) {
                test.execute("select 1");
            }
            if (!connection.getAutoCommit()) connection.commit();
            return true;
        } catch (SQLException e) {
            log.warn("A connection failed validation, closing it: " + e.toString(), e);
            destroy();
        }
        return false;
    }

    @Override
    public void setReadOnly(final boolean readOnly) {
        try {
            connection.setReadOnly(readOnly);
        } catch (SQLException e) {
            final String reason = "Failed to set read-only status: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) {
        try {
            return connection.prepareStatement(sql);
        } catch (SQLException e) {
            final String reason = "Failed to prepare a statement: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType, final int resultSetConcurrency) {
        try {
            return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            final String reason = "Failed to prepare a statement: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public Statement createStatement() {
        try {
            return connection.createStatement();
        } catch (SQLException e) {
            final String reason = "Failed to create a statement: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) {
        try {
            return connection.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException e) {
            final String reason = "Failed to create a statement: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public void execute(final String sql) {
        final Statement statement = createStatement();
        try {
            try {
                statement.execute(sql);
            }
            catch (SQLException e) {
                final String reason = "Failed to execute SQL statement \"" + sql.replace("\"", "\\\"") + "\": " + e.toString();
                log.error(reason, e);
                throw new DatabaseException(reason, e);
            }
        }
        finally {
            try {
                statement.close();
            } catch (SQLException e) {
                final String reason = "Failed to close a statement: " + e.toString();
                log.error(reason, e);
                // do not throw an exception, as we do not consider this a fatal error
            }
        }
    }

    @Override
    public CallableStatement prepareCall(final String sql) {
        try {
            return connection.prepareCall(sql);
        } catch (SQLException e) {
            final String reason = "Failed to create a statement: " + e.toString();
            log.error(reason, e);
            throw new DatabaseException(reason, e);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
