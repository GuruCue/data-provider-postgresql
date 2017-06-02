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
package com.gurucue.recommendations.test;

import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.blender.DataSet;
import com.gurucue.recommendations.blender.VideoData;
import com.gurucue.recommendations.caching.CachedJdbcDataProvider;
import com.gurucue.recommendations.data.postgresql.PostgreSqlDataProvider;
import com.gurucue.recommendations.data.jdbc.JdbcDataLink;
import com.gurucue.recommendations.data.jdbc.JdbcDataProvider;
import com.gurucue.recommendations.dto.ConsumerEntity;
import com.gurucue.recommendations.dto.RelationConsumerProductEntity;
import com.gurucue.recommendations.entity.Partner;
import com.gurucue.recommendations.entitymanager.ConsumerManager;
import com.gurucue.recommendations.entitymanager.ProductManager;
import com.gurucue.recommendations.entitymanager.TvProgrammeInfo;
import com.gurucue.recommendations.entitymanager.VideoInfo;
import org.junit.Test;

import java.util.List;

/**
 * Simple initial tests of the provider.
 */
public class InstantiationTests {
    private static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/demo";
    private static final String DB_USERNAME = "demo";
    private static final String DB_PASSWORD = "demo";

    @Test
    public void testLink() {
        try (final JdbcDataProvider provider = PostgreSqlDataProvider.create(DB_URL, DB_USERNAME, DB_PASSWORD);) {
            try (final JdbcDataLink link = provider.newJdbcDataLink()) {
                try (final Transaction transaction = Transaction.newTransaction(link)) {
                    final Partner p = link.getPartnerManager().getById(6L);
                    final ProductManager pm = link.getProductManager();
                    final ConsumerManager cm = link.getConsumerManager();

                    final long now = System.currentTimeMillis();
                    long startNano = System.nanoTime();
                    final List<TvProgrammeInfo> tvProgrammes = pm.tvProgrammesInIntervalForPartner(transaction, p, now, 259200000L, 300000L);
                    long endNano = System.nanoTime();
                    System.out.println("Ordinary provider: obtained a list of " + tvProgrammes.size() + " TV programmes in " + (endNano - startNano) + " ns");

                    startNano = System.nanoTime();
                    final List<VideoInfo> vods = pm.vodForPartner(transaction, p, now);
                    endNano = System.nanoTime();
                    System.out.println("Ordinary provider: obtained a list of " + vods.size() + " VODs in " + (endNano - startNano) + " ns");

                    ConsumerEntity c = cm.getByPartnerIdAndUsernameAndTypeAndParent(6L, MY_USERNAME, 1L, 0L);
                    if (c == null) System.out.println("Ordinary provider: consumer " + MY_USERNAME + " does not exist");
                    else {
                        System.out.println("Ordinary provider: consumer " + MY_USERNAME + " has ID " + c.id + " and " + c.relations.size() + " subscriptions:");
                        for (final RelationConsumerProductEntity r : c.relations) {
                            System.out.println("  " + r.productId + " since " + r.relationStart + " till " + r.relationEnd);
                        }
                    }
                }
            }
        }
    }

    private static final String MY_USERNAME = "3d6d7e57142d09c5e89ee333e9c7a962";

    @Test
    public void testCachedLink() {
        JdbcDataProvider provider = PostgreSqlDataProvider.create(DB_URL, DB_USERNAME, DB_PASSWORD);
        try (final CachedJdbcDataProvider cachedProvider = CachedJdbcDataProvider.create(provider)) {
            try (final JdbcDataLink link = cachedProvider.newJdbcDataLink()) {
                try (final Transaction transaction = Transaction.newTransaction(link)) {
                    final Partner p = link.getPartnerManager().getById(6L);
                    final ProductManager pm = link.getProductManager();
                    final ConsumerManager cm = link.getConsumerManager();

                    final long now = System.currentTimeMillis();
                    long startNano = System.nanoTime();
                    final List<TvProgrammeInfo> tvProgrammes = pm.tvProgrammesInIntervalForPartner(transaction, p, now, 259200000L, 300000L);
                    long endNano = System.nanoTime();
                    System.out.println("Cached provider: obtained a list of " + tvProgrammes.size() + " TV programmes in " + (endNano - startNano) + " ns");

                    startNano = System.nanoTime();
                    final List<VideoInfo> vods = pm.vodForPartner(transaction, p, now);
                    endNano = System.nanoTime();
                    System.out.println("Cached provider: obtained a list of " + vods.size() + " VODs in " + (endNano - startNano) + " ns");

                    ConsumerEntity c = cm.getByPartnerIdAndUsernameAndTypeAndParent(6L, MY_USERNAME, 1L, 0L);
                    if (c == null) System.out.println("Consumer " + MY_USERNAME + " does not exist");
                    else {
                        System.out.println("Cached provider: consumer " + MY_USERNAME + " has ID " + c.id + " and " + c.relations.size() + " subscriptions:");
                        for (final RelationConsumerProductEntity r : c.relations) {
                            System.out.println("  " + r.productId + " since " + r.relationStart + " till " + r.relationEnd);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testVideoDataDataSetBuild() {
        JdbcDataProvider provider = PostgreSqlDataProvider.create(DB_URL, DB_USERNAME, DB_PASSWORD);
        try (final CachedJdbcDataProvider cachedProvider = CachedJdbcDataProvider.create(provider)) {
            try (final JdbcDataLink link = cachedProvider.newJdbcDataLink()) {
                try (final Transaction transaction = Transaction.newTransaction(link)) {
                    final ConsumerManager cm = link.getConsumerManager();
                    final Partner p = link.getPartnerManager().getById(6L);
                    final ConsumerEntity c = cm.getByPartnerIdAndUsernameAndTypeAndParent(6L, MY_USERNAME, 1L, 0L);
                    final DataSet<VideoData> ds = VideoData.buildDataSetOfVideosAndTvProgrammes(transaction, p, c, (value1, value2) -> value1, System.currentTimeMillis(), 4L * 24L * 60L * 60L * 1000L, 0L);
                    System.out.println("Received a dataset of size " + ds.size());
                }
            }
        }
    }
}
