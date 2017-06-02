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

import com.gurucue.recommendations.Timer;
import com.gurucue.recommendations.TimerListener;
import com.gurucue.recommendations.Transaction;
import com.gurucue.recommendations.TransactionalEntity;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.TvChannelProduct;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class TransactionalTvProgrammeEntity extends TransactionalEntity<Product> implements TimerListener {
    private static final Logger log = LogManager.getLogger(TransactionalTvProgrammeEntity.class);

    protected final PartnerProductCache cache;
    protected long expiryMillis;

    protected TransactionalTvProgrammeEntity(final PartnerProductCache cache, final TvProgrammeProduct committedValue) {
        super(committedValue);
        this.cache = cache;
        setTimer(null, committedValue);
    }

    protected TransactionalTvProgrammeEntity(final PartnerProductCache cache, final TvProgrammeProduct uncommittedValue, final Transaction transaction) {
        super(uncommittedValue, transaction);
        this.cache = cache;
    }

    public static TransactionalTvProgrammeEntity createCommitted(final PartnerProductCache cache, final TvProgrammeProduct value) {
        return new TransactionalTvProgrammeEntity(cache, value);
    }

    public static TransactionalTvProgrammeEntity createInTransaction(final PartnerProductCache cache, final TvProgrammeProduct value, final Transaction transaction) {
        return new TransactionalTvProgrammeEntity(cache, value, transaction);
    }

    public void setCurrentValue(final Product value) {
        final TvProgrammeProduct oldValue = (TvProgrammeProduct)committed;
        final TvProgrammeProduct newValue = (TvProgrammeProduct)value;
        super.setCurrentValue(value);
        setTimer(oldValue, newValue);
    }

    protected synchronized void setTimer(final TvProgrammeProduct oldValue, final TvProgrammeProduct newValue) {
        if (oldValue == null) {
            if (newValue == null) return;
            expiryMillis = newValue.beginTimeMillis + maxCatchupMillis(newValue);
            Timer.INSTANCE.schedule(expiryMillis, this);
        }
        else if (newValue == null) {
            Timer.INSTANCE.unschedule(expiryMillis, this);
        }
        else if (oldValue.beginTimeMillis != newValue.beginTimeMillis) {
            Timer.INSTANCE.unschedule(expiryMillis, this);
            expiryMillis = newValue.beginTimeMillis + maxCatchupMillis(newValue);
            Timer.INSTANCE.schedule(expiryMillis, this);
        }
    }

    public static final long TV_PROGRAMME_CACHING_RESERVE_HOURS = 24L;
    public static final long TV_PROGRAMME_CACHING_RESERVE_MILLIS = TV_PROGRAMME_CACHING_RESERVE_HOURS * 60L * 60L * 1000L;
    public static final long TV_PROGRAMME_CACHING_DEFAULT_MILLIS = 96L * 60L * 60L * 1000L; // for tv-programmes that don't have a (valid) tv-channel

    protected long maxCatchupMillis(final TvProgrammeProduct tvProgramme) {
        final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
        // if not mapped to a tv-channel, then it's not reachable, so no catch-up
        if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) return 0L;
        long maxCatchup = -1L;
        for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
            final TvChannelProduct tvChannel = cache.getTvChannelByProductCode(tvChannelCodes[i]);
            if (tvChannel == null) continue;
            if (tvChannel.catchupMillis > maxCatchup) maxCatchup = tvChannel.catchupMillis;
        }
        return maxCatchup < 0L ? TV_PROGRAMME_CACHING_DEFAULT_MILLIS : maxCatchup + TV_PROGRAMME_CACHING_RESERVE_MILLIS;
    }

    @Override
    public void onTimerExpired(final long expiryTime) {
        if (committed == null) log.debug("Tv-programme expired: (null)");
        else if (!(committed instanceof TvProgrammeProduct)) log.debug("Tv-programme expired: (not a TvProgrammeProduct instance) " + committed.id);
        else {
            final TvProgrammeProduct tvProgramme = (TvProgrammeProduct)committed;
            final StringBuilder logBuilder = new StringBuilder(256);
            logBuilder.append("TV-programme expired: ");
            if (tvProgramme.title == null) logBuilder.append("(null title)");
            else logBuilder.append(tvProgramme.title.value);
            logBuilder.append(" on channels [");
            if (tvProgramme.tvChannelCodes != null) {
                final int n = tvProgramme.tvChannelCodes.length;
                for (int i = 0; i < n; i++) logBuilder.append(tvProgramme.tvChannelCodes[i]);
            }
            logBuilder.append("] running between ").append(tvProgramme.beginTimeMillis / 1000L).append(" and ").append(tvProgramme.endTimeMillis / 1000L);
            log.debug(logBuilder.toString());
        }
    }
}
