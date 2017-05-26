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

package org.apache.ignite.ml.math.impls.storage.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.MatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.CacheUtils;
import org.apache.ignite.ml.math.impls.matrix.BlockEntry;

/**
 * TODO: add description.
 * TODO: WIP.
 */
public class BlockMatrixStorage extends CacheUtils implements MatrixStorage, StorageConstants {
    /** Cache name used for all instances of {@link BlockMatrixStorage}.*/
    public static final String ML_BLOCK_CACHE_NAME = "ML_BLOCK_SPARSE_MATRICES_CONTAINER";
    /** Amount of rows in the matrix. */
    private int rows;
    /** Amount of columns in the matrix. */
    private int cols;
    /** Matrix uuid. */
    private IgniteUuid uuid;
    /** Block size about 8 KB of data. */
    private int blockSize = 32 * 32;

    /** Actual distributed storage. */
    private IgniteCache<
        BlockMatrixKey /* Matrix block number with uuid. */,
        BlockEntry /* Block of matrix, local sparse matrix. */
        > cache = null;

    /**
     *
     */
    public BlockMatrixStorage() {
        // No-op.
    }

    /**
     * @param rows Amount of rows in the matrix.
     * @param cols Amount of columns in the matrix.
     */
    public BlockMatrixStorage(int rows, int cols) {
        assert rows > 0;
        assert cols > 0;

        this.rows = rows;
        this.cols = cols;

        cache = newCache();

        uuid = IgniteUuid.randomUuid();

        initBlocks();
    }

    /**
     *  Create new ML cache if needed.
     */
    private IgniteCache<BlockMatrixKey, BlockEntry> newCache() {
        CacheConfiguration<BlockMatrixKey, BlockEntry> cfg = new CacheConfiguration<>();

        // Write to primary.
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No eviction.
        cfg.setEvictionPolicy(null);

        // No copying of values.
        cfg.setCopyOnRead(false);

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        // Random cache name.
        cfg.setName(ML_BLOCK_CACHE_NAME);

        IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(cfg);

        return cache;
    }

    /**
     *
     */
    public IgniteCache<BlockMatrixKey, BlockEntry> cache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return matrixGet(x, y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        matrixSet(x, y, v);
    }

    /** */
    private void initBlocks(){
        Map<BlockMatrixKey, BlockEntry> all = new HashMap<>();
        long id = 0l;

        for(int i = 0; i < rows; i+=32)
            for(int j = 0; j < cols; j+=32){
                int blockRows = rows - i >= 32 ? 32 : rows - i;
                int blockCols = cols - j >= 32 ? 32 : cols - j;

                BlockEntry entry = new BlockEntry(blockCols, blockRows);
                BlockMatrixKey key = new BlockMatrixKey(id, getUUID(), null);

                all.put(key, entry);

                id++;
            }

        cache.putAll(all);
    }

    /** */
    private long getBlockId(int x, int y){
        return (y / 32 + y % 32 > 0 ? 1 : 0) * (cols / 32 + cols % 32 > 0 ? 1 : 0) + (x / 32 + (x % 32 > 0 ? 1 : 0)  + 1);
    }

    /**
     * Distributed matrix get.
     *
     * @param a Row or column index.
     * @param b Row or column index.
     * @return Matrix value at (a, b) index.
     */
    private double matrixGet(int a, int b) {
        // Remote get from the primary node (where given row or column is stored locally).
        return ignite().compute(groupForKey(ML_BLOCK_CACHE_NAME, getBlockId(a, b))).call(() -> {
            IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(ML_BLOCK_CACHE_NAME);

            BlockMatrixKey key = getCacheKey(getBlockId(a, b));

            // Local get.
            BlockEntry block = cache.localPeek(key, CachePeekMode.PRIMARY);

            if (block == null)
                block = cache.get(key);

            return block.get(a % 32, b % 32);
        });
    }

    private BlockMatrixKey getCacheKey(long blockId) {
        return new BlockMatrixKey(blockId, uuid, null);
    }

    /**
     * Distributed matrix set.
     *
     * @param a Row or column index.
     * @param b Row or column index.
     * @param v New value to set.
     */
    private void matrixSet(int a, int b, double v) {
        long id = getBlockId(a, b);
        System.out.println("block id " + id);
        // Remote set on the primary node (where given row or column is stored locally).
        ignite().compute(groupForKey(ML_BLOCK_CACHE_NAME, id)).run(() -> {
            IgniteCache<BlockMatrixKey, BlockEntry> cache = Ignition.localIgnite().getOrCreateCache(ML_BLOCK_CACHE_NAME);

            BlockMatrixKey key = getCacheKey(getBlockId(a, b));

            // Local get.
            BlockEntry block = cache.localPeek(key, CachePeekMode.PRIMARY);

            if (block == null)
                block = cache.get(key); //Remote entry get.

            block.set(a % 32, b % 32, v);

            // Local put.
            cache.put(key, block);
        });
    }


    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);
        U.writeGridUuid(out, uuid);
        out.writeUTF(cache.getName());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();
        uuid = U.readGridUuid(in);
        cache = ignite().getOrCreateCache(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** Delete all data from cache. */
    @Override public void destroy() {
        long maxBlockId = getBlockId(cols, rows);

        Set<BlockMatrixKey> keyset = LongStream.rangeClosed(0, maxBlockId).mapToObj(this::getCacheKey).collect(Collectors.toSet());

        cache.clearAll(keyset);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + cols;
        res = res * 37 + rows;
        res = res * 37 + uuid.hashCode();
        res = res * 37 + cache.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        BlockMatrixStorage that = (BlockMatrixStorage)obj;

        return rows == that.rows && cols == that.cols && uuid.equals(that.uuid)
            && (cache != null ? cache.equals(that.cache) : that.cache == null);
    }

    /** */
    public IgniteUuid getUUID() {
        return uuid;
    }

}
