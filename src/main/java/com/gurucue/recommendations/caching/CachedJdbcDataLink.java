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
package com.gurucue.recommendations.caching;

import com.gurucue.recommendations.TransactionCloseJob;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.entitymanager.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Wraps the given JdbcDataLink with a caching layer, it abstracts a
 * transaction handle.
 * Note: this is not thread-safe; it is meant to be used as one instance per
 * thread, but there is nothing stopping you to use it otherwise.
 */
public final class CachedJdbcDataLink implements JdbcDataLink {
    private static final Logger log = LogManager.getLogger(CachedJdbcDataLink.class);

    private JdbcDataLink physicalLink;
    final CachedJdbcDataProvider provider;
    @Deprecated // TODO: remove
    final List<Runnable> commitJobs = new ArrayList<>();
    final LinkedList<TransactionCloseJob> transactionCloseJobs = new LinkedList<>();

    private ProductManagerCached productManager = null;
    private ConsumerManagerCached consumerManager = null;

    CachedJdbcDataLink(final CachedJdbcDataProvider provider) {
        this.provider = provider;
        this.physicalLink = null;
    }

    JdbcDataLink getPhysicalLink() {
        // lazy init: we don't need physical provider for cached data
        if (physicalLink == null) physicalLink = provider.newPhysicalLink();
        return physicalLink;
    }

    @Override
    public boolean isValid() {
        return getPhysicalLink().isValid();
    }

    @Override
    public void setReadOnly(final boolean b) {
        this.setReadOnly(b);
    }

    @Override
    public PreparedStatement prepareStatement(final String s) {
        return getPhysicalLink().prepareStatement(s);
    }

    @Override
    public PreparedStatement prepareStatement(final String s, final int i, final int i2) {
        return getPhysicalLink().prepareStatement(s, i, i2);
    }

    @Override
    public Statement createStatement() {
        return getPhysicalLink().createStatement();
    }

    @Override
    public Statement createStatement(final int i, final int i2) {
        return getPhysicalLink().createStatement(i, i2);
    }

    @Override
    public void execute(final String s) {
        getPhysicalLink().execute(s);
    }

    @Override
    public CallableStatement prepareCall(final String s) {
        return getPhysicalLink().prepareCall(s);
    }

    @Override
    public Connection getConnection() {
        return getPhysicalLink().getConnection();
    }

    @Override
    public void close() {
        commitJobs.clear();
        transactionCloseJobs.clear();
        getPhysicalLink().close();
    }

    @Override
    public void commit() {
        getPhysicalLink().commit();
        for (final Runnable job : commitJobs) {
            try {
                job.run();
            }
            catch (Throwable e) {
                log.error("A commit job failed, after the database transaction was committed: " + e.toString(), e);
            }
        }
        for (final TransactionCloseJob job : transactionCloseJobs) {
            try {
                job.commit();
            }
            catch (Throwable e) {
                log.error("Commit on a transaction-close job failed, after the database transaction was committed: " + e.toString(), e);
            }
        }
        transactionCloseJobs.clear();
        commitJobs.clear();
    }

    @Deprecated // TODO: remove
    public void addCommitJob(final Runnable job) {
        commitJobs.add(job);
    }

    public void onTransactionClose(final TransactionCloseJob job) {
        transactionCloseJobs.addFirst(job);
    }

    @Override
    public void rollback() {
        for (final TransactionCloseJob job : transactionCloseJobs) {
            try {
                job.rollback();
            }
            catch (Throwable e) {
                log.error("Rollback on a transaction-close job failed, before the database transaction was rolled back: " + e.toString(), e);
            }
        }
        transactionCloseJobs.clear();
        commitJobs.clear();
        getPhysicalLink().rollback();
    }

    @Override
    public DataProvider getProvider() {
        return provider;
    }

    @Override
    public Partner getPartnerZero() {
        return getPhysicalLink().getPartnerZero();
    }

    @Override
    public AttributeManager getAttributeManager() {
        return getPhysicalLink().getAttributeManager();
    }

    @Override
    public ConsumerEventManager getConsumerEventManager() {
        return getPhysicalLink().getConsumerEventManager();
    }

    @Override
    public ConsumerEventTypeManager getConsumerEventTypeManager() {
        return getPhysicalLink().getConsumerEventTypeManager();
    }

    @Override
    public ConsumerManager getConsumerManager() {
        if (consumerManager == null) consumerManager = new ConsumerManagerCached(this, provider.consumerCache);
        return consumerManager;
    }

    @Override
    public DataTypeManager getDataTypeManager() {
        return getPhysicalLink().getDataTypeManager();
    }

    @Override
    public LanguageManager getLanguageManager() {
        return getPhysicalLink().getLanguageManager();
    }

    @Override
    public LogSvcRecommendationManager getLogSvcRecommendationManager() {
        return getPhysicalLink().getLogSvcRecommendationManager();
    }

    @Override
    public PartnerManager getPartnerManager() {
        return getPhysicalLink().getPartnerManager();
    }

    @Override
    public PartnerRecommenderManager getPartnerRecommenderManager() {
        return getPhysicalLink().getPartnerRecommenderManager();
    }

    @Override
    public ProductManager getProductManager() {
        if (productManager == null) productManager = new ProductManagerCached(this);
        return productManager;
    }

    @Override
    public ProductTypeManager getProductTypeManager() {
        return getPhysicalLink().getProductTypeManager();
    }

    @Override
    public ProductTypeAttributeManager getProductTypeAttributeManager() {
        return getPhysicalLink().getProductTypeAttributeManager();
    }

    @Override
    public RecommenderConsumerOverrideManager getRecommenderConsumerOverrideManager() {
        return getPhysicalLink().getRecommenderConsumerOverrideManager();
    }

    @Override
    public RecommenderManager getRecommenderManager() {
        return getPhysicalLink().getRecommenderManager();
    }

    @Override
    public RelationTypeManager getRelationTypeManager() {
        return getPhysicalLink().getRelationTypeManager();
    }

    @Override
    public LogSvcSearchManager getLogSvcSearchManager() {
        return getPhysicalLink().getLogSvcSearchManager();
    }
}
