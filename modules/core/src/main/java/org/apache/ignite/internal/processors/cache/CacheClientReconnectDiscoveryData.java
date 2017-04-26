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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteUuid;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class CacheClientReconnectDiscoveryData implements Serializable {
    /** */
    private final Map<String, CacheInfo> clientCaches;

    public CacheClientReconnectDiscoveryData(Map<String, CacheInfo> clientCaches) {
        this.clientCaches = clientCaches;
    }

    Map<String, CacheInfo> clientCaches() {
        return clientCaches;
    }

    static class CacheInfo implements Serializable {
        /** */
        private final CacheConfiguration ccfg;

        /** */
        private final CacheType cacheType;

        /** */
        private final IgniteUuid deploymentId;

        /** */
        private final boolean nearCache;

        /** */
        private final byte flags;

        public CacheInfo(CacheConfiguration ccfg,
            CacheType cacheType,
            IgniteUuid deploymentId,
            boolean nearCache,
            byte flags) {
            assert ccfg != null;
            assert cacheType != null;
            assert deploymentId != null;

            this.ccfg = ccfg;
            this.cacheType = cacheType;
            this.deploymentId = deploymentId;
            this.nearCache = nearCache;
            this.flags = flags;
        }

        CacheConfiguration config() {
            return ccfg;
        }

        CacheType cacheType() {
            return cacheType;
        }

        IgniteUuid deploymentId() {
            return deploymentId;
        }

        boolean nearCache() {
            return nearCache;
        }
    }
}
