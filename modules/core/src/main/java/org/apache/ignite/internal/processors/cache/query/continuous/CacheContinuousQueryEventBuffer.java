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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteSystemProperties;
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
    private AtomicReference<Batch> curBatch = new AtomicReference<>();

    /** */
    private ConcurrentSkipListMap<Long, Object> pending = new ConcurrentSkipListMap<>();

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
     * @return Collected entries to pass to listener (single entry or entries list).
     */
    @Nullable Object processEntry(CacheContinuousQueryEntry e) {
        return process0(e.updateCounter(), e);
    }

    /**
     * @param cntr Filtered counter.
     * @return Collected entries to pass to listener (single entry or entries list).
     */
    @Nullable Object processFiltered(long cntr) {
        return process0(cntr, cntr);
    }

    /**
     * @param cntr Entry counter.
     * @param entry Entry.
     * @return Collected entries.
     */
    private Object process0(long cntr, Object entry) {
        assert cntr >= 0 : cntr;

        Batch batch = initBatch();

        if (batch == null || cntr < batch.startCntr) {
            assert entry != null : cntr;

            return entry;
        }

        Object res = null;

        if (cntr <= batch.endCntr)
            res = batch.processEvent0(null, cntr, entry);
        else
            pending.put(cntr, entry);

        Batch batch0 = curBatch.get();

        if (batch0 != batch) {
            do {
                batch = batch0;

                res = processPending(res, batch);

                batch0 = curBatch.get();
            }
            while (batch != batch0);
        }

        return res;
    }

    /**
     * @return Current batch.
     */
    @Nullable private Batch initBatch() {
        Batch batch = curBatch.get();

        if (batch != null)
            return batch;

        long curCntr = currentPartitionCounter();

        if (curCntr == -1)
            return null;

        batch = new Batch(curCntr + 1, 0L, new Object[BUF_SIZE]);

        if (curBatch.compareAndSet(null, batch))
            return batch;

        return curBatch.get();
    }

    /**
     * @param res Current result.
     * @param batch Current batch.
     * @return New result.
     */
    @Nullable private Object processPending(@Nullable Object res, Batch batch) {
        if (pending.floorKey(batch.endCntr) != null) {
            for (Map.Entry<Long, Object> p : pending.headMap(batch.endCntr, true).entrySet()) {
                long cntr = p.getKey();

                assert cntr >= batch.startCntr && cntr <= batch.endCntr : cntr;

                if (pending.remove(p.getKey()) != null)
                    res = batch.processEvent0(res, p.getKey(), p.getValue());
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
        private final Object[] evts;

        /**
         * @param filtered Number of filtered events before this batch.
         * @param evts Events array.
         * @param startCntr Start counter.
         */
        Batch(long startCntr, long filtered, Object[] evts) {
            assert startCntr >= 0;
            assert filtered >= 0;

            this.startCntr = startCntr;
            this.filtered = filtered;
            this.evts = evts;

            endCntr = startCntr + BUF_SIZE - 1;
        }

        /**
         * @param res Current result.
         * @param cntr Event counter.
         * @param evt Event.
         * @return New result.
         */
        @SuppressWarnings("unchecked")
        @Nullable private Object processEvent0(
            @Nullable Object res,
            long cntr,
            Object evt) {
            int pos = (int)(cntr - startCntr);

            synchronized (this) {
                evts[pos] = evt;

                int next = lastProc + 1;

                if (next == pos) {
                    for (int i = next; i < evts.length; i++) {
                        Object e = evts[i];

                        if (e != null) {
                            if (e.getClass() == Long.class)
                                filtered++;
                            else {
                                CacheContinuousQueryEntry evt0 = (CacheContinuousQueryEntry)e;

                                if (!evt0.isFiltered()) {
                                    evt0.filteredCount(filtered);

                                    filtered = 0;

                                    if (res == null)
                                        res = evt0;
                                    else {
                                        List<CacheContinuousQueryEntry> resList;

                                        if (res instanceof CacheContinuousQueryEntry) {
                                            resList = new ArrayList<>();

                                            resList.add((CacheContinuousQueryEntry)res);
                                        }
                                        else {
                                            assert res instanceof List : res;

                                            resList = (List<CacheContinuousQueryEntry>)res;
                                        }

                                        resList.add(evt0);

                                        res = resList;
                                    }
                                }
                                else
                                    filtered++;
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

            if (pos == evts.length -1) {
                Arrays.fill(evts, null);

                Batch nextBatch = new Batch(this.startCntr + BUF_SIZE, filtered, evts);

                curBatch.set(nextBatch);
            }

            return res;
        }
    }
}