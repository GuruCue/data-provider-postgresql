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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.gurucue.recommendations.ProcessingException;
import com.gurucue.recommendations.blender.DataSet;
import com.gurucue.recommendations.blender.TvChannelData;
import com.gurucue.recommendations.blender.VideoData;
import com.gurucue.recommendations.data.ProductTypeCodes;
import com.gurucue.recommendations.entity.product.PackageProduct;
import com.gurucue.recommendations.entity.product.Product;
import com.gurucue.recommendations.entity.product.TvChannelProduct;
import com.gurucue.recommendations.entity.product.TvProgrammeProduct;
import gnu.trove.set.TLongSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Indexes tv-programmes according to their begin-time.
 * TODO: add eviction timer, or rely on upstream evictions?
 */
public class TvProgrammeBeginIndex implements Index<Long, Product> {
    private static final Logger log = LogManager.getLogger(TvProgrammeBeginIndex.class);

    private final PartnerProductCache cache;
    private final long productTypeIdForTvChannel;
    private final long productTypeIdForTvProgramme;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock(false);
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private BeginTimeIndex index;

    public TvProgrammeBeginIndex(final PartnerProductCache cache) {
        this.cache = cache;
        final ProductTypeCodes productTypeCodes = cache.dataProvider.getProductTypeCodes();
        productTypeIdForTvChannel = productTypeCodes.idForTvChannel;
        productTypeIdForTvProgramme = productTypeCodes.idForTvProgramme;
        index = BeginTimeIndex.create(cache);
    }

    public void addTvChannels(final Collection<String> tvChannelCodes) {
        if ((tvChannelCodes == null) || (tvChannelCodes.isEmpty())) return;
        final long startTime = System.nanoTime();
        writeLocked(() -> {
            index = BeginTimeIndex.appendTvChannels(index, tvChannelCodes);
            return (Void) null;
        });
        final long stopTime = System.nanoTime();
        final StringBuilder logBuilder = new StringBuilder(tvChannelCodes.size() * 32 + 128); // guesstimate
        logBuilder.append("Added TV-channels: ");
        final Iterator<String> it = tvChannelCodes.iterator();
        if (it.hasNext()) {
            logBuilder.append(it.next());
            while (it.hasNext()) logBuilder.append(", ").append(it.next());
        }
        else logBuilder.append("(none)");
        logBuilder.append("; timing: ").append(stopTime - startTime).append(" ns");
        log.debug(logBuilder.toString());
    }

    @Override
    public void replaceAll(final Collection<Product> products) {
        // fill
        int countNullPartnerProduct = 0;
        int countDeletedProducts = 0;
        int countTvChannels = 0;
        int countTvProgrammes = 0;
        final long tvChannelId = this.productTypeIdForTvChannel; // make frequently accessed variables local
        final long tvProgrammeId = this.productTypeIdForTvProgramme;
        final Set<String> tvChannelCodes = new HashSet<>();
        final List<TvProgrammeProduct> tvProgrammes = new LinkedList<>();

        final long startTime = System.nanoTime();
        for (final Product product : products) {
            if (product == null) {
                countNullPartnerProduct++;
                continue;
            }
            if (product.deleted != null) {
                // skip deleted products
                countDeletedProducts++;
                continue;
            }

            final long productTypeId = product.productTypeId;
            if (productTypeId == tvProgrammeId) {
                try {
                    tvProgrammes.add((TvProgrammeProduct) product);
                    countTvProgrammes++;
                } catch (ClassCastException e) {
                    log.error("A TV-programme is not of class TvProgrammeProduct but " + product.getClass().getCanonicalName() + ": " + e.toString(), e);
                }
            }
            else if (productTypeId == tvChannelId) {
                countTvChannels++;
                tvChannelCodes.add(product.partnerProductCode);
            }
        }

        final long indexTime = System.nanoTime();
        final BeginTimeIndex newIndex = BeginTimeIndex.create(cache, tvChannelCodes);
        newIndex.addTvProgrammes(tvProgrammes);

        final long stopTime = System.nanoTime();
        final StringBuilder logBuilder = new StringBuilder(1024 + (tvChannelCodes.size() * 64)); // guesstimate
        logBuilder.append("Constructed TvProgrammeBeginIndex for partner ").append(cache.partnerId)
                .append(", timing: ").append(indexTime - startTime).append(" ns sorting, ").append(stopTime - indexTime)
                .append(" ns indexing; encountered null Products: ")
                .append(countNullPartnerProduct)
                .append(", deleted products: ")
                .append(countDeletedProducts)
                .append(", tv-channels: ")
                .append(countTvChannels).append(" (").append(tvChannelCodes.size()).append(" unique)")
                .append(", tv-programmes: ")
                .append(countTvProgrammes).append(", with tv-channels and number of their tv-programmes:");
        newIndex.logTvChannels(logBuilder);
        log.debug(logBuilder.toString());

        writeLocked(() -> {
            index = newIndex;
            return (Void) null;
        });
    }

    @Override
    public void changed(final Product before, final Product after) {
        // assumption: before and after have the same partner and tv-channel
        if (after == null) {
            // deletion
            if (before == null) return;

            final long productTypeId = before.productTypeId;
            if (productTypeId == productTypeIdForTvChannel) {
                changedTvChannel((TvChannelProduct) before, null);
            }
            else if (productTypeId == productTypeIdForTvProgramme) {
                changedTvProgramme((TvProgrammeProduct) before, null);
            }
        }

        else if (before == null) {
            // insertion

            final long productTypeId = after.productTypeId;
            if (productTypeId == productTypeIdForTvChannel) {
                changedTvChannel(null, (TvChannelProduct) after);
            }
            else if (productTypeId == productTypeIdForTvProgramme) {
                changedTvProgramme(null, (TvProgrammeProduct) after);
            }
        }

        else {
            // update

            final long productTypeId = after.productTypeId;
            if (productTypeId == productTypeIdForTvChannel) {
                // assumption: before and after tv-channels have the same code
                changedTvChannel((TvChannelProduct) before, (TvChannelProduct) after);
            }
            else if (productTypeId == productTypeIdForTvProgramme) {
                changedTvProgramme((TvProgrammeProduct) before, (TvProgrammeProduct) after);
            }
        }
    }

    @Override
    public IndexDefinition getDefinition() {
        throw new UnsupportedOperationException("getDefinition() not supported");
    }

    @Override
    public void internalRemove(Long key, Product entity) {
        throw new UnsupportedOperationException("internalRemove() not supported");
    }

    @Override
    public void internalAdd(Long key, Product entity) {
        throw new UnsupportedOperationException("internalAdd() not supported");
    }

    @Override
    public void internalReplace(Long key, Product before, Product after) {
        throw new UnsupportedOperationException("internalReplace() not supported");
    }

    @Override
    public void destroy() {

    }

    void changedTvChannel(final TvChannelProduct before, final TvChannelProduct after) {
        // as far as TV-channels are concerned we care only for partner_product_code, which is used as an index for caching tv-programmes
        if (after == null) {
            // deletion
            if (before == null) {
                // this cannot happen in real-world, but we include it for completeness
                log.error("The index was notified with both, before and after, TV-channel states null, which is not correct");
                return;
            }
            if (before.partnerProductCode == null) {
                log.error("The removed TV-channel with ID=" + before.id + " has a null partner_product_code");
                return;
            }
            final long startTime = System.nanoTime();
            writeLocked(() -> {
                index = BeginTimeIndex.removeTvChannel(index, before.partnerProductCode);
                return (Void) null;
            });
            final long stopTime = System.nanoTime();
            log.debug("Removed TV-channel from index: " + before.partnerProductCode + ", timing: " + (stopTime - startTime) + " ns");
        }
        else if (before == null) {
            // addition: add this tv-channel's partner_product_code
            if (after.partnerProductCode == null) {
                log.error("The new TV-channel with ID=" + after.id + " has a null partner_product_code");
                return;
            }
            if (index.tvChannels.containsKey(after.partnerProductCode)) {
                log.error("The index was notified about the addition of a new TV-channel with ID=" + after.id + ", partner_id=" + after.partnerId + ", partner_product_code=" + after.partnerProductCode + ", but a TV-channel with such partner_product_code already exists in the index");
                return;
            }
            writeLocked(() -> {
                index = BeginTimeIndex.appendTvChannel(index, after.partnerProductCode);
                return (Void) null;
            });
        }
        else {
            // modification - there's nothing to do since partner_product_code is not mutable, we only perform a few checks on it
            if (before.partnerProductCode == null) log.error("The old TV-channel instance has a null partner_product_code");
            if (after.partnerProductCode == null) log.error("The new TV-channel instance has a null partner_product_code");
            if ((before.partnerProductCode != null) && (after.partnerProductCode != null) && !before.partnerProductCode.equals(after.partnerProductCode)) {
                log.error("The partner_product_code of the new and old TV-channel instances differ: " + before.partnerProductCode + " != " + after.partnerProductCode + ", ID=" + before.id);
            }
        }
    }

    void changedTvProgramme(final TvProgrammeProduct before, final TvProgrammeProduct after) {
        if (after == null) {
            // deletion
            if (before == null) {
                log.error("The index was notified with both, before and after, TV-programme states null, which is not correct");
                return;
            }
            readLocked(() -> {
                index.removeTvProgramme(before);
                return (Void) null;
            });
        }
        else if (before == null) {
            // addition
            readLocked(() -> {
                index.setTvProgramme(after);
                return (Void) null;
            });
        }
        else {
            // modification: here be careful: the set of TV-channels for the TV-programme may have changed
            final String[] oldCodes = before.tvChannelCodes;
            final String[] newCodes = after.tvChannelCodes;
            boolean isDifferent = oldCodes.length != newCodes.length;
            if (!isDifferent) {
                // see the difference to decide whether they are different
                final int n = oldCodes.length;
                if (n == 1) {
                    // optimization for the case of a single TV-channel, which is the majority
                    isDifferent = !oldCodes[0].equals(newCodes[0]);
                }
                else {
                    final Set<String> difference = new HashSet<>();
                    for (int i = n - 1; i >= 0; i--) difference.add(oldCodes[i]);
                    for (int i = n - 1; i >= 0; i--) difference.remove(newCodes[i]);
                    isDifferent = difference.size() > 0;
                }
            }
            if (isDifferent) {
                // first remove, then add
                readLocked(() -> {
                    index.removeTvProgramme(before);
                    index.setTvProgramme(after);
                    return (Void)null;
                });
            }
            else {
                // just overwrite
                readLocked(() -> {
                    index.setTvProgramme(after);
                    return (Void) null;
                });
            }
        }
    }

    public void reachableTvProgrammesInRange(
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final DataSet.Builder<VideoData> outputBuilder
    ) {
        // first retrieve the index, then invoke it
        final BeginTimeIndex index = getIndex();
        final PackageDetails details = index.tvChannelDatas(negativeOffsetMillis, subscriptionPackageIds);
        final DataSetOutput output = new DataSetOutput(details.tvChannelDatas, outputBuilder);
        index.reachableTvProgrammesInRange(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, details, output);
    }

    public void reachableTvProgrammesInRange(
            final long recommendTimeMillis,
            final long negativeOffsetMillis,
            final long positiveOffsetMillis,
            final TLongSet subscriptionPackageIds,
            final List<VideoData> outputList
    ) {
        // first retrieve the index, then invoke it
        final BeginTimeIndex index = getIndex();
        final PackageDetails details = index.tvChannelDatas(negativeOffsetMillis, subscriptionPackageIds);
        final ListOutput output = new ListOutput(details.tvChannelDatas, outputList);
        index.reachableTvProgrammesInRange(recommendTimeMillis, negativeOffsetMillis, positiveOffsetMillis, details, output);
    }

    public SortedSet<TvProgrammeProduct> tvProgrammesOverlapping(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis) {
        // first retrieve the index, then invoke it
        return getIndex().tvProgrammesOverlapping(tvChannelCode, beginTimeMillis, endTimeMillis);
    }

    public void forEachOverlappingTvProgramme(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
        getIndex().forEachOverlappingTvProgramme(tvChannelCode, beginTimeMillis, endTimeMillis, consumer);
    }

    public TvProgrammeProduct tvProgrammeAt(final String tvChannelCode, final long timeMillis) {
        // first retrieve the index, then invoke it
        return getIndex().tvProgrammeAt(tvChannelCode, timeMillis);
    }

    public TvProgrammeProduct firstTvProgrammeAfter(final String tvChannelCode, final long timeMillis) {
        // first retrieve the index, then invoke it
        return getIndex().firstTvProgrammeAfter(tvChannelCode, timeMillis);
    }

    private BeginTimeIndex getIndex() {
        try {
            readLock.lockInterruptibly();
            try {
                return this.index;
            } finally {
                readLock.unlock();
            }
        } catch (InterruptedException e) {
            throw new ProcessingException("[" + Thread.currentThread().getId() + "] Interrupted while waiting on read lock: " + e.toString(), e);
        }
    }

    private <A> A writeLocked(final Callable<A> worker) {
        try {
            writeLock.lockInterruptibly();
            try {
                return worker.call();
            } catch (Exception e) {
                throw new ProcessingException("[" + Thread.currentThread().getId() + "] Invocation of worker failed: " + e.toString(), e);
            } finally {
                writeLock.unlock();
            }
        } catch (InterruptedException e) {
            throw new ProcessingException("[" + Thread.currentThread().getId() + "] Interrupted while waiting on write lock: " + e.toString(), e);
        }
    }

    private <A> A readLocked(final Callable<A> worker) {
        try {
            readLock.lockInterruptibly();
            try {
                return worker.call();
            } catch (Exception e) {
                throw new ProcessingException("[" + Thread.currentThread().getId() + "] Invocation of worker failed: " + e.toString(), e);
            } finally {
                readLock.unlock();
            }
        } catch (InterruptedException e) {
            throw new ProcessingException("[" + Thread.currentThread().getId() + "] Interrupted while waiting on read lock: " + e.toString(), e);
        }
    }

    /**
     * The actual index.
     */
    static final class BeginTimeIndex {
        final PartnerProductCache cache;
        final String[] orderedTvChannels;
        final TvChannelBeginTimeIndex[] subIndexes;
        final int tvChannelCount; // tvChannelCount == orderedTvChannels.lengtgh == tvChannels.size()
        final ImmutableMap<String, Integer> tvChannels; // maps: code -> index into orderedTvChannels

        private BeginTimeIndex(final PartnerProductCache cache, final String[] orderedTvChannels) {
            this.cache = cache;
            this.orderedTvChannels = orderedTvChannels;
            tvChannels = mapTvChannelCodesToIndexes(orderedTvChannels);
            tvChannelCount = orderedTvChannels.length;
            subIndexes = new TvChannelBeginTimeIndex[tvChannelCount];
            for (int i = tvChannelCount - 1; i >= 0; i--) subIndexes[i] = new TvChannelBeginTimeIndex();
        }

        static BeginTimeIndex create(final PartnerProductCache cache) {
            return new BeginTimeIndex(cache, new String[0]);
        }

        static BeginTimeIndex create(final PartnerProductCache cache, final Set<String> tvChannelCodesCollection) { // don't use any other Collection: a Set prevents presence of duplicates
            final int n = tvChannelCodesCollection.size();
            final String[] tvChannelCodes = tvChannelCodesCollection.toArray(new String[n]);
            Arrays.sort(tvChannelCodes);
            return new BeginTimeIndex(cache, tvChannelCodes);
        }

        /**
         * Constructor that takes the old index and makes a new one with the provided
         * additional tv-channel. No checks are made.
         *
         * @param oldIndex the old BeginTimeIndex instance
         * @param newTvChannelCode the tv-channel code to add
         * @return new index instance
         */
        static BeginTimeIndex appendTvChannel(final BeginTimeIndex oldIndex, final String newTvChannelCode) {
            final int n = oldIndex.tvChannelCount;
            if (oldIndex.tvChannels.containsKey(newTvChannelCode)) return oldIndex; // no adding necessary
            final String[] orderedTvChannels = Arrays.copyOf(oldIndex.orderedTvChannels, n + 1);
            orderedTvChannels[n] = newTvChannelCode;
            Arrays.sort(orderedTvChannels);
            final BeginTimeIndex result = new BeginTimeIndex(oldIndex.cache, orderedTvChannels);
            result.copyEntriesFromIndex(oldIndex);
            return result;
        }

        /**
         * Constructor that takes the old index and makes a new one with the provided
         * additional tv-channels. Duplicate tv-channels are removed.
         *
         * @param oldIndex the old BeginTimeIndex instance
         * @param newTvChannelCodes the tv-channel codes to add
         * @return new index instance
         */
        static BeginTimeIndex appendTvChannels(final BeginTimeIndex oldIndex, final Collection<String> newTvChannelCodes) {
            // filter out duplicates from the new codes
            final Set<String> processedNewCodes = new HashSet<>();
            final Map<String, Integer> oldTvChannels = oldIndex.tvChannels;
            newTvChannelCodes.forEach((final String code) -> {
                if (!oldTvChannels.containsKey(code)) processedNewCodes.add(code);
            });
            // add the filtered codes
            final int n = oldIndex.tvChannelCount;
            final int additionalSize = processedNewCodes.size();
            final String[] orderedTvChannels = Arrays.copyOf(oldIndex.orderedTvChannels, n + additionalSize);
            int i = n;
            for (final String code : processedNewCodes) {
                orderedTvChannels[i++] = code;
            }
            Arrays.sort(orderedTvChannels);
            // construct the result
            final BeginTimeIndex result = new BeginTimeIndex(oldIndex.cache, orderedTvChannels);
            result.copyEntriesFromIndex(oldIndex);
            return result;
        }

        static BeginTimeIndex removeTvChannel(final BeginTimeIndex oldIndex, final String removedTvChannelCode) {
            final Integer removedIndexObj = oldIndex.tvChannels.get(removedTvChannelCode);
            if (removedIndexObj == null) return oldIndex; // no adding necessary
            final int removedIndex = removedIndexObj.intValue();
            final int n = oldIndex.tvChannelCount;
            final String[] orderedTvChannels = new String[n-1];
            final String[] oldOrderedTvChannels = oldIndex.orderedTvChannels;
            for (int i = 0; i < removedIndex; i++) {
                orderedTvChannels[i] = oldOrderedTvChannels[i];
            }
            for (int i = removedIndex + 1; i < n; i++) {
                orderedTvChannels[i - 1] = oldOrderedTvChannels[i];
            }
            final BeginTimeIndex result = new BeginTimeIndex(oldIndex.cache, orderedTvChannels);
            result.copyEntriesFromIndex(oldIndex);
            return result;
        }

        void addTvProgrammes(final Collection<TvProgrammeProduct> tvProgrammes) {
            final ImmutableMap<String, Integer> tvChannels = this.tvChannels; // cache the variable locally for faster access
            tvProgrammes.forEach((final TvProgrammeProduct tvProgramme) -> {
                final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
                if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                    log.error("Cannot index the TV-programme ID=" + tvProgramme.id + ": it belongs to no TV-channel");
                    return;
                }
                for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
                    final Integer idxObj = tvChannels.get(tvChannelCodes[i]);
                    if (idxObj == null) continue;
                    subIndexes[idxObj.intValue()].set(tvProgramme);
                }
            });
        }

        void removeTvProgramme(final TvProgrammeProduct tvProgramme) {
            final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
            if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                log.error("Cannot remove the TV-programme ID=" + tvProgramme.id + ": it belongs to no TV-channel");
                return;
            }
            for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
                final Integer index = tvChannels.get(tvChannelCodes[i]);
                if (index == null) continue;
                subIndexes[index.intValue()].remove(tvProgramme);
            }
        }

        void setTvProgramme(final TvProgrammeProduct tvProgramme) {
            final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
            if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) {
                log.error("Cannot index the TV-programme ID=" + tvProgramme.id + ": it belongs to no TV-channel");
                return;
            }
            for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
                final Integer index = tvChannels.get(tvChannelCodes[i]);
                if (index == null) continue;
                subIndexes[index.intValue()].set(tvProgramme);
            }
        }

        void destroy() {
        }

        void logTvChannels(final StringBuilder output) {
            final int n = tvChannelCount; // local variable reference, for faster access
            final String[] codes = this.orderedTvChannels;
            final TvChannelBeginTimeIndex[] subIndexes = this.subIndexes;
            for (int i = 0; i < n; i++) {
                output.append("\n    ").append(codes[i]).append(": ").append(subIndexes[i].size());
            }
        }

        PackageDetails tvChannelDatas(
                final long negativeOffsetMillis,
                final TLongSet subscriptionPackageIds
        ) {
            final Map<String, TvChannelData> packageMapping = cache.currentTvChannelCodeToPackageMapping();
            final Map<String, TvChannelData> tvChannelDatas = new HashMap<>();
            final long[] catchups = new long[tvChannelCount]; // fills with zeros by default, which is okay
            packageMapping.forEach((final String tvChannelCode, final TvChannelData tvChannelData) -> {
                // computes TvChannelData instances and correct catchupMillis per TV-channel
                final Integer idxObj = tvChannels.get(tvChannelCode);
                if (idxObj == null) {
                    // the given code is not mapped
                    log.error("Cannot retrieve tv-programmes for tv-channel " + tvChannelCode + ": it is not mapped to an internal index: ");
                    return;
                }
                final int tvChannelIndex = idxObj.intValue();
                if (subIndexes[tvChannelIndex].isEmpty()) {
                    // no tv-programmes cached for this tv-channel
                    return;
                }
                final long catchupMillis = tvChannelData.tvChannel.catchupMillis;
                catchups[tvChannelIndex] = catchupMillis < negativeOffsetMillis ? catchupMillis : negativeOffsetMillis;
                boolean isSubscribed = false;
                for (final PackageProduct p : tvChannelData.productPackages) {
                    if ((subscriptionPackageIds != null) && subscriptionPackageIds.contains(p.id)) {
                        isSubscribed = true;
                        break;
                    }
                }
                tvChannelDatas.put(tvChannelCode, new TvChannelData(tvChannelData.tvChannel, tvChannelData.productPackages, isSubscribed));
            });
            return new PackageDetails(tvChannelDatas, catchups);
        }

        void reachableTvProgrammesInRange(
                final long recommendTimeMillis,
                final long negativeOffsetMillis,
                final long positiveOffsetMillis,
                final PackageDetails packageDetails,
                final CachingOutput outputter
        ) {
            final long startMillis = System.nanoTime();
            final int startCount = outputter.size();
            final long[] catchups = packageDetails.catchups; // fills with zeros by default, which is okay

            final int n = tvChannelCount;
            final TvChannelBeginTimeIndex[] subIndexes = this.subIndexes; // cache for faster local access
            final Long recommendTimeObj = Long.valueOf(recommendTimeMillis);
            final Long stopTimeObj = Long.valueOf(recommendTimeMillis + positiveOffsetMillis);
            final StringBuilder channelLogBuilder = new StringBuilder(n * 512); // guesstimate
            final LinkedList<String> emptyEpg = new LinkedList<>();
            int sizeBefore = startCount;
            int rawTotal = 0;
            for (int i = 0; i < n; i++) {
                final int rawCount = subIndexes[i].addSchedule(recommendTimeObj, recommendTimeMillis - catchups[i], stopTimeObj, outputter);
                rawTotal += rawCount;
                final int sizeAfter = outputter.size();
                if (rawCount == 0) emptyEpg.add(orderedTvChannels[i]);
                else channelLogBuilder.append("\n  ").append(orderedTvChannels[i]).append(": ").append(rawCount).append(", deduplicated: ").append(sizeAfter - sizeBefore);
                sizeBefore = sizeAfter;
            }

            final long stopMillis = System.nanoTime();
            final int stopCount = outputter.size();
            final StringBuilder logBuilder = new StringBuilder(1024);
            logBuilder.append("Assembled ").append(stopCount - startCount)
                    .append(" tv-programmes (raw count: ").append(rawTotal).append(") at ").append(recommendTimeMillis)
                    .append(" with deltaT-: ").append(negativeOffsetMillis)
                    .append(" and deltaT+: ").append(positiveOffsetMillis)
                    .append(": ").append(stopMillis - startMillis)
                    .append(" ns, number of tv-programmes per tv-channel:").append(channelLogBuilder);
            if (!emptyEpg.isEmpty()) {
                logBuilder.append("\nThe following tv-channels had no tv-programmes in the time range: ").append(emptyEpg.removeFirst());
                emptyEpg.forEach((final String tvCode) -> logBuilder.append(", ").append(tvCode));
            }
            log.debug(logBuilder.toString());
        }

        void copyEntriesFromIndex(final BeginTimeIndex beginTimeIndex) {
            for (int i = tvChannelCount - 1; i >= 0; i--) {
                final Integer idxObj = beginTimeIndex.tvChannels.get(orderedTvChannels[i]);
                if (idxObj == null) continue; // no such tv-channel in the given index
                subIndexes[i].copyFrom(beginTimeIndex.subIndexes[idxObj.intValue()]);
            }
        }

        private static ImmutableMap<String, Integer> mapTvChannelCodesToIndexes(final String[] tvChannelCodes) {
            final ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
            for (int i = tvChannelCodes.length - 1; i >= 0; i--) {
                builder.put(tvChannelCodes[i], Integer.valueOf(i));
            }
            return builder.build();
        }

        SortedSet<TvProgrammeProduct> tvProgrammesOverlapping(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis) {
            final Integer channelIndex = tvChannels.get(tvChannelCode);
            if (channelIndex == null) {
                log.warn("tvProgrammesOverlapping(): there is no TV-channel \"" + tvChannelCode + "\"");
                return Collections.emptySortedSet();
            }
            return subIndexes[channelIndex].tvProgrammesOverlapping(beginTimeMillis, endTimeMillis);
        }

        void forEachOverlappingTvProgramme(final String tvChannelCode, final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
            final Integer channelIndex = tvChannels.get(tvChannelCode);
            if (channelIndex == null) {
                log.warn("forEachOverlappingTvProgramme(): there is no TV-channel \"" + tvChannelCode + "\"");
                return;
            }
            subIndexes[channelIndex].forEachOverlappingTvProgramme(beginTimeMillis, endTimeMillis, consumer);
        }

        TvProgrammeProduct tvProgrammeAt(final String tvChannelCode, final long timeMillis) {
            final Integer channelIndex = tvChannels.get(tvChannelCode);
            if (channelIndex == null) {
                log.warn("tvProgrammeAt(): there is no TV-channel \"" + tvChannelCode + "\"");
                return null;
            }
            return subIndexes[channelIndex].tvProgrammeAt(timeMillis);
        }

        TvProgrammeProduct firstTvProgrammeAfter(final String tvChannelCode, final long timeMillis) {
            final Integer channelIndex = tvChannels.get(tvChannelCode);
            if (channelIndex == null) {
                log.warn("firstTvProgrammeAfter(): there is no TV-channel \"" + tvChannelCode + "\"");
                return null;
            }
            return subIndexes[channelIndex].firstTvProgrammeAfter(timeMillis);
        }
    }

    /**
     * The sub-index, for a specific TV-channel.
     */
    static final class TvChannelBeginTimeIndex {
        private final TreeMap<Long, TvProgrammeProduct> schedule = new TreeMap<>(); // begin-time -> tv-programme product
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock(false);
        private final Lock readLock = rwLock.readLock();
        private final Lock writeLock = rwLock.writeLock();

        void copyFrom(final TvChannelBeginTimeIndex subIndex) {
            try {
                writeLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.writeLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                schedule.putAll(subIndex.schedule);
            }
            finally {
                writeLock.unlock();
            }
        }

        void set(final TvProgrammeProduct tvProgramme) {
            try {
                writeLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.writeLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                schedule.put(tvProgramme.beginTimeMillis, tvProgramme);
            }
            finally {
                writeLock.unlock();
            }
        }

        void remove(final TvProgrammeProduct tvProgramme) {
            try {
                writeLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.writeLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                schedule.remove(tvProgramme.beginTimeMillis);
            }
            finally {
                writeLock.unlock();
            }
        }

        boolean isEmpty() {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                return schedule.isEmpty();
            }
            finally {
                readLock.unlock();
            }
        }

        private int addSchedule(final Long nowMillis, final Long startMillis, final Long stopMillis, final CachingOutput outputter) {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                if (schedule.isEmpty()) return 0;
                final NavigableMap<Long, TvProgrammeProduct> rangeSchedule = schedule.subMap(startMillis, true, stopMillis, true);
                int count = rangeSchedule.size();
                if (nowMillis < startMillis) {
                    // the given range is in the future
                    // include the playing tv-show at the startMillis, if it ends within the range
                    final TvProgrammeProduct currentTvProgramme = currentTvProgramme(startMillis);
                    if ((currentTvProgramme != null) && (currentTvProgramme.endTimeMillis > startMillis)) {
                        outputter.output(currentTvProgramme);
                        count++;
                    }
                    rangeSchedule.forEach((final Long timestamp, final TvProgrammeProduct tvProgramme) -> outputter.output(tvProgramme));
                }
                else {
                    // the given range may include the catchup
                    final Spliterator<TvProgrammeProduct> s = rangeSchedule.values().spliterator();
                    final TvProgrammeProduct[] tmp = new TvProgrammeProduct[1]; // we need a container, because we use a lambda
                    if (s.tryAdvance((final TvProgrammeProduct tvProgramme) -> tmp[0] = tvProgramme)) {
                        final TvProgrammeProduct firsTvProgramme = tmp[0];
                        if (firsTvProgramme.beginTimeMillis > nowMillis) {
                            // first include the currently playing tv-programme
                            final TvProgrammeProduct tvProgramme = currentTvProgramme(nowMillis);
                            if (tvProgramme != null) {
                                outputter.output(tvProgramme);
                                count++;
                            }
                        }
                        outputter.output(firsTvProgramme);
                        s.forEachRemaining((final TvProgrammeProduct tvProgramme) -> outputter.output(tvProgramme));
                    }
                    else if (stopMillis >= nowMillis) {
                        // empty range, which includes now: try to find a tv-programme that is playing now
                        final TvProgrammeProduct tvProgramme = currentTvProgramme(nowMillis);
                        if (tvProgramme != null) {
                            outputter.output(tvProgramme);
                            count++;
                        }
                    }
                }
                return count;
            }
            finally {
                readLock.unlock();
            }
        }

        int addSchedule(final Long nowMillis, final Long startMillis, final Long stopMillis, final Map<String, TvChannelData> tvChannelDataMap, final DataSet.Builder<VideoData> outputBuilder) {
            return addSchedule(nowMillis, startMillis, stopMillis, new DataSetOutput(tvChannelDataMap, outputBuilder));
        }

        int addSchedule(final Long nowMillis, final Long startMillis, final Long stopMillis, final Map<String, TvChannelData> tvChannelDataMap, final List<VideoData> list) {
            return addSchedule(nowMillis, startMillis, stopMillis, new ListOutput(tvChannelDataMap, list));
        }

        int size() {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                return schedule.size();
            }
            finally {
                readLock.unlock();
            }
        }

        private TvProgrammeProduct currentTvProgramme(final Long nowMillis) {
            final Map.Entry<Long, TvProgrammeProduct> currentEntry = schedule.floorEntry(nowMillis);
            if (currentEntry == null) return null;
            final TvProgrammeProduct result = currentEntry.getValue();
            if (result.endTimeMillis < nowMillis) return null; // ends before the time
            return result;
        }

        SortedSet<TvProgrammeProduct> tvProgrammesOverlapping(final Long beginTimeMillis, final Long endTimeMillis) {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                if (schedule.isEmpty()) return Collections.emptySortedSet();
                final NavigableMap<Long, TvProgrammeProduct> rangeSchedule = schedule.subMap(beginTimeMillis, true, endTimeMillis, false);
                final SortedSet<TvProgrammeProduct> result = new TreeSet<>(TvProgrammeBeginTimeEquality.INSTANCE);
                final Map.Entry<Long, TvProgrammeProduct> firstEntry = rangeSchedule.firstEntry();
                // if there is no tv-programme in range, or the first tv-programme in range begins after the start of
                // the interval, then also look at the preceding tv-programme
                if ((firstEntry == null) || (firstEntry.getValue().beginTimeMillis > beginTimeMillis)) {
                    final Map.Entry<Long, TvProgrammeProduct> beforeEntry = schedule.floorEntry(beginTimeMillis);
                    if ((beforeEntry != null) && (beforeEntry.getValue().endTimeMillis > beginTimeMillis)) result.add(beforeEntry.getValue());
                }
                // add all tv-programmes in the range
                rangeSchedule.forEach((final Long beginTime, final TvProgrammeProduct tvProgrammeProduct) -> result.add(tvProgrammeProduct));
                return result;
            }
            finally {
                readLock.unlock();
            }
        }

        void forEachOverlappingTvProgramme(final Long beginTimeMillis, final Long endTimeMillis, final Consumer<TvProgrammeProduct> consumer) {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                if (schedule.isEmpty()) return;
                final NavigableMap<Long, TvProgrammeProduct> rangeSchedule = schedule.subMap(beginTimeMillis, true, endTimeMillis, false);
                final Map.Entry<Long, TvProgrammeProduct> firstEntry = rangeSchedule.firstEntry();
                // if there is no tv-programme in range, or the first tv-programme in range begins after the start of
                // the interval, then also look at the preceding tv-programme
                if ((firstEntry == null) || (firstEntry.getValue().beginTimeMillis > beginTimeMillis)) {
                    final Map.Entry<Long, TvProgrammeProduct> beforeEntry = schedule.floorEntry(beginTimeMillis);
                    if ((beforeEntry != null) && (beforeEntry.getValue().endTimeMillis > beginTimeMillis)) consumer.accept(beforeEntry.getValue());
                }
                // add all tv-programmes in the range
                rangeSchedule.forEach((final Long beginTime, final TvProgrammeProduct tvProgrammeProduct) -> consumer.accept(tvProgrammeProduct));
            }
            finally {
                readLock.unlock();
            }
        }

        TvProgrammeProduct tvProgrammeAt(final long timeMillis) {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                if (schedule.isEmpty()) return null;
                if (timeMillis < schedule.firstKey().longValue()) return ProductCache.TV_PROGRAMME_TIME_OUT_OF_RANGE; // not in cached range
                final Map.Entry<Long, TvProgrammeProduct> beforeEntry = schedule.floorEntry(timeMillis);
                if ((beforeEntry == null) || (beforeEntry.getValue().endTimeMillis <= timeMillis)) return null;
                return beforeEntry.getValue();
            }
            finally {
                readLock.unlock();
            }
        }

        TvProgrammeProduct firstTvProgrammeAfter(final long timeMillis) {
            try {
                readLock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new ProcessingException("Interrupted while trying to lock TvChannelBeginTimeIndex.readLock, threadId=" + Thread.currentThread().getId(), e);
            }
            try {
                if (schedule.isEmpty()) return null;
                if (timeMillis < schedule.firstKey().longValue()) return ProductCache.TV_PROGRAMME_TIME_OUT_OF_RANGE; // not in cached range
                final Map.Entry<Long, TvProgrammeProduct> afterEntry = schedule.higherEntry(timeMillis);
                if (afterEntry == null) return null;
                return afterEntry.getValue();
            }
            finally {
                readLock.unlock();
            }
        }
    }

    private interface CachingOutput {
        void output(TvProgrammeProduct tvProgramme);
        int size();
    }

    private static abstract class VideoDataOutput implements CachingOutput {
        private final Map<String, DataCacheEntry> cache = new HashMap<>();
        private final Map<String, TvChannelData> tvChannelDataMap;
        private final StringBuilder keyBuilder = new StringBuilder(64);

        VideoDataOutput(final Map<String, TvChannelData> tvChannelDataMap) {
            this.tvChannelDataMap = tvChannelDataMap;
        }

        protected abstract void add(VideoData data);

        public void output(final TvProgrammeProduct tvProgramme) {
            final String[] tvChannelCodes = tvProgramme.tvChannelCodes;
            if ((tvChannelCodes == null) || (tvChannelCodes.length == 0)) return;
            final String key;
            if (tvChannelCodes.length == 1) {
                key = tvChannelCodes[0];
            }
            else {
                // assemble the key with codes as they are provided: we can do this, because TvProgrammeProduct guarantees sorted ordering
                final int n = tvChannelCodes.length;
                keyBuilder.setLength(0);
                keyBuilder.append(tvChannelCodes[0]);
                for (int i = 1; i < n; i++) keyBuilder.append("#").append(tvChannelCodes[i]);
                key = keyBuilder.toString();
            }
            final DataCacheEntry existingEntry = cache.get(key);
            if (existingEntry != null) {
                add(existingEntry.createVideoData(tvProgramme));
                return;
            }
            // it is not cached yet, process it and cache it
            boolean isSubscribed = false;
            final int n = tvChannelCodes.length;
            final ImmutableList.Builder<TvChannelData> builder = ImmutableList.builder();
            for (int i = 0; i < n; i++) {
                final TvChannelData tvData = tvChannelDataMap.get(tvChannelCodes[i]);
                if (tvData == null) continue;
                if (tvData.isSubscribed) isSubscribed = true;
                builder.add(tvData);
            }
            final DataCacheEntry newEntry = new DataCacheEntry(builder.build(), isSubscribed);
            cache.put(key, newEntry);
            add(newEntry.createVideoData(tvProgramme));
        }
    }

    private static final class DataSetOutput extends VideoDataOutput {
        private final DataSet.Builder<VideoData> outputBuilder;

        DataSetOutput(final Map<String, TvChannelData> tvChannelDataMap, final DataSet.Builder<VideoData> outputBuilder) {
            super(tvChannelDataMap);
            this.outputBuilder = outputBuilder;
        }

        @Override
        protected void add(final VideoData data) {
            outputBuilder.add(data);
        }

        @Override
        public int size() {
            return outputBuilder.size();
        }
    }

    private static final class ListOutput extends VideoDataOutput {
        private final List<VideoData> list;

        ListOutput(final Map<String, TvChannelData> tvChannelDataMap, final List<VideoData> list) {
            super(tvChannelDataMap);
            this.list = list;
        }

        @Override
        protected void add(final VideoData data) {
            list.add(data);
        }

        @Override
        public int size() {
            return list.size();
        }
    }

    private static final class DataCacheEntry {
        final List<TvChannelData> tvChannels;
        final boolean isSubscribed;
        DataCacheEntry(final List<TvChannelData> tvChannels, final boolean isSubscribed) {
            this.tvChannels = tvChannels;
            this.isSubscribed = isSubscribed;
        }
        VideoData createVideoData(final TvProgrammeProduct tvProgramme) {
            return VideoData.forTvProgramme(tvProgramme, tvChannels, isSubscribed);
        }
    }

    private static final class PackageDetails {
        final Map<String, TvChannelData> tvChannelDatas;
        final long[] catchups;

        PackageDetails(final Map<String, TvChannelData> tvChannelDatas, final long[] catchups) {
            this.tvChannelDatas = tvChannelDatas;
            this.catchups = catchups;
        }
    }
}
