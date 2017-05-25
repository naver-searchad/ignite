/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.communication.tcp.TestDebugLog;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheContinuousQueryEventBuffer {
    /** */
    // TODO increase to 1000
    private static final int BUF_SIZE =
        IgniteSystemProperties.getInteger("IGNITE_CONTINUOUS_QUERY_SERVER_BUFFER_SIZE", 5);

    /** */
    protected final int part;

    /** */
    private AtomicReference<Batch> curBatch = new AtomicReference<>();

    /** */
    private ConcurrentLinkedDeque<CacheContinuousQueryEntry> backupQ = new ConcurrentLinkedDeque<>();

    /** */
    private ConcurrentSkipListMap<Long, CacheContinuousQueryEntry> pending = new ConcurrentSkipListMap<>();

    /**
     * @param part Partition number.
     */
    CacheContinuousQueryEventBuffer(int part) {
        this.part = part;
    }

    /**
     * @param updateCntr Acknowledged counter.
     */
    void cleanupBackupQueue(Long updateCntr) {
        Iterator<CacheContinuousQueryEntry> it = backupQ.iterator();

        while (it.hasNext()) {
            CacheContinuousQueryEntry backupEntry = it.next();

            if (backupEntry.updateCounter() <= updateCntr)
                it.remove();
        }
    }

    /**
     * @return Backup entries.
     */
    @Nullable Collection<CacheContinuousQueryEntry> resetBackupQueue() {
        Collection<CacheContinuousQueryEntry> ret;

        List<CacheContinuousQueryEntry> entries = null;

        Batch batch = curBatch.get();

        if (batch != null)
            entries = batch.backupFlushEntries();

        if (!backupQ.isEmpty()) {
            if (entries != null)
                backupQ.addAll(entries);

            ret = this.backupQ;

            backupQ = new ConcurrentLinkedDeque<>();
        }
        else
            ret = entries;

        if (ret != null) {
            for (CacheContinuousQueryEntry e : ret)
                TestDebugLog.addEntryMessage(part,
                    e.updateCounter(),
                    "filtered " + e.filteredCount() +
                        " reset backup");
        }

        return ret;
    }

    /**
     * @return Initial partition counter.
     */
    protected long currentPartitionCounter() {
        return 0;
    }

    /**
     * For test purpose only.
     *
     * @return Current number of filtered events.
     */
    long currentFiltered() {
        Batch batch = curBatch.get();

        return batch != null ? batch.filtered : 0;
    }

    /**
     * @param e Entry to process.
     * @param backup Backup entry flag.
     * @return Collected entries to pass to listener (single entry or entries list).
     */
    @Nullable Object processEntry(CacheContinuousQueryEntry e, boolean backup) {
        return process0(e.updateCounter(), e, backup);
    }

    /**
     * @param backup Backup entry flag.
     * @param cntr Entry counter.
     * @param entry Entry.
     * @return Collected entries.
     */
    private Object process0(long cntr, CacheContinuousQueryEntry entry, boolean backup) {
        assert cntr >= 0 : cntr;

        Batch batch = initBatch(entry.topologyVersion());

        if (batch == null || cntr < batch.startCntr) {
            if (backup)
                backupQ.add(entry);

            TestDebugLog.addEntryMessage(part,
                cntr,
                "buffer rcd small start=" + batch.startCntr +
                    " cntr=" + cntr +
                    ", backup=" + backup +
                    " topVer=" + ((CacheContinuousQueryEntry)entry).topologyVersion());

            return entry;
        }

        Object res = null;

        if (cntr <= batch.endCntr)
            res = batch.processEvent0(null, cntr, entry, backup);
        else {
            TestDebugLog.addEntryMessage(part,
                cntr,
                "buffer add pending start=" + batch.startCntr +
                    " cntr=" + cntr +
                    " topVer=" + ((CacheContinuousQueryEntry)entry).topologyVersion());
            pending.put(cntr, entry);
        }

        Batch batch0 = curBatch.get();

        if (batch0 != batch) {
            do {
                batch = batch0;

                res = processPending(res, batch, backup);

                batch0 = curBatch.get();
            }
            while (batch != batch0);
        }

        return res;
    }

    /**
     * @param topVer Current event topology version.
     * @return Current batch.
     */
    @Nullable private Batch initBatch(AffinityTopologyVersion topVer) {
        Batch batch = curBatch.get();

        if (batch != null)
            return batch;

        long curCntr = currentPartitionCounter();

        if (curCntr == -1)
            return null;

        TestDebugLog.addEntryMessage(part, curCntr, "created batch");

        batch = new Batch(curCntr + 1, 0L, new CacheContinuousQueryEntry[BUF_SIZE], topVer);

        if (curBatch.compareAndSet(null, batch))
            return batch;

        return curBatch.get();
    }

    /**
     * @param res Current result.
     * @param batch Current batch.
     * @param backup Backup entry flag.
     * @return New result.
     */
    @Nullable private Object processPending(@Nullable Object res, Batch batch, boolean backup) {
        if (pending.floorKey(batch.endCntr) != null) {
            for (Map.Entry<Long, CacheContinuousQueryEntry> p : pending.headMap(batch.endCntr, true).entrySet()) {
                long cntr = p.getKey();

                assert cntr >= batch.startCntr && cntr <= batch.endCntr : cntr;

                if (pending.remove(p.getKey()) != null)
                    res = batch.processEvent0(res, p.getKey(), p.getValue(), backup);
            }
        }

        return res;
    }

    /**
     *
     */
    private class Batch {
        /** */
        private long filtered;

        /** */
        private final long startCntr;

        /** */
        private final long endCntr;

        /** */
        private int lastProc = -1;

        /** */
        private final CacheContinuousQueryEntry[] entries;

        /** */
        private final AffinityTopologyVersion topVer;

        /**
         * @param filtered Number of filtered events before this batch.
         * @param entries Entries array.
         * @param topVer Current event topology version.
         * @param startCntr Start counter.
         */
        Batch(long startCntr, long filtered, CacheContinuousQueryEntry[] entries, AffinityTopologyVersion topVer) {
            assert startCntr >= 0;
            assert filtered >= 0;

            this.startCntr = startCntr;
            this.filtered = filtered;
            this.entries = entries;
            this.topVer = topVer;

            endCntr = startCntr + BUF_SIZE - 1;
        }

        /**
         * @return Entries to send as part of backup queue.
         */
        @Nullable synchronized List<CacheContinuousQueryEntry> backupFlushEntries() {
            List<CacheContinuousQueryEntry> res = null;

            long filtered = this.filtered;
            long cntr = startCntr;

            for (int i = 0; i < entries.length; i++) {
                CacheContinuousQueryEntry e = entries[i];

                CacheContinuousQueryEntry flushEntry = null;

                if (e == null) {
                    if (filtered != 0) {
                        flushEntry = filteredEntry(cntr - 1, filtered - 1);

                        filtered = 0;
                    }
                }
                else {
                    if (e.isFiltered())
                        filtered++;
                    else {
                        flushEntry = new CacheContinuousQueryEntry(e.cacheId(),
                            e.eventType(),
                            e.key(),
                            e.value(),
                            e.oldValue(),
                            e.isKeepBinary(),
                            e.partition(),
                            e.updateCounter(),
                            e.topologyVersion());

                        flushEntry.filteredCount(filtered);

                        filtered = 0;
                    }
                }

                if (flushEntry != null) {
                    if (res == null)
                        res = new ArrayList<>();

                    res.add(flushEntry);
                }

                cntr++;
            }

            if (filtered != 0L) {
                if (res == null)
                    res = new ArrayList<>();

                res.add(filteredEntry(cntr - 1, filtered - 1));
            }

            return res;
        }

        /**
         * @param cntr Entry counter.
         * @param filtered Number of entries filtered before this entry.
         * @return Entry.
         */
        private CacheContinuousQueryEntry filteredEntry(long cntr, long filtered) {
            CacheContinuousQueryEntry e = new CacheContinuousQueryEntry(0,
                null,
                 null,
                 null,
                 null,
                 false,
                 part,
                 cntr,
                 topVer);

            e.markFiltered();

            e.filteredCount(filtered);

            return e;
        }

        /**
         * @param res Current result.
         * @param cntr Entry counter.
         * @param entry Entry.
         * @param backup Backup entry flag.
         * @return New result.
         */
        @SuppressWarnings("unchecked")
        @Nullable private Object processEvent0(
            @Nullable Object res,
            long cntr,
            CacheContinuousQueryEntry entry,
            boolean backup) {
            int pos = (int)(cntr - startCntr);

            synchronized (this) {
                TestDebugLog.addEntryMessage(part,
                    cntr,
                    "buffer process start=" + startCntr +
                        ", lastProc=" + lastProc +
                        " pos=" + pos +
                        " topVer=" + entry.topologyVersion());

                entries[pos] = entry;

                int next = lastProc + 1;

                if (next == pos) {
                    for (int i = next; i < entries.length; i++) {
                        CacheContinuousQueryEntry entry0 = entries[i];

                        if (entry0 != null) {
                            if (!entry0.isFiltered()) {
                                TestDebugLog.addEntryMessage(part,
                                    cntr,
                                    "buffer process res start=" + startCntr +
                                        ", lastProc=" + lastProc +
                                        " pos=" + pos +
                                        ", filtered=" + filtered +
                                        " topVer=" + entry0.topologyVersion());

                                entry0.filteredCount(filtered);

                                filtered = 0;

                                if (res == null) {
                                    if (backup)
                                        backupQ.add(entry0);
                                    else
                                        res = entry0;
                                }
                                else {
                                    assert !backup;

                                    List<CacheContinuousQueryEntry> resList;

                                    if (res instanceof CacheContinuousQueryEntry) {
                                        resList = new ArrayList<>();

                                        resList.add((CacheContinuousQueryEntry)res);
                                    }
                                    else {
                                        assert res instanceof List : res;

                                        resList = (List<CacheContinuousQueryEntry>)res;
                                    }

                                    resList.add(entry0);

                                    res = resList;
                                }
                            }
                            else {
                                filtered++;

                                TestDebugLog.addEntryMessage(part,
                                    cntr,
                                    "buffer process inc filtered start=" + startCntr +
                                        ", lastProc=" + lastProc +
                                        " pos=" + pos +
                                        ", filtered=" + filtered +
                                        " topVer=" + entry0.topologyVersion());
                            }

                            pos = i;
                        }
                        else
                            break;
                    }

                    lastProc = pos;
                }
                else
                    return res;
            }

            if (pos == entries.length -1) {
                Arrays.fill(entries, null);

                Batch nextBatch = new Batch(this.startCntr + BUF_SIZE, filtered, entries, entry.topologyVersion());

                curBatch.set(nextBatch);
            }

            return res;
        }
    }
}