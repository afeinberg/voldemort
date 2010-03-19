/*
 * Copyright 2008-2009 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.memory;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Identical to the InMemoryStorageConfiguration except that it creates a heap aware backing map which evicts
 * entries once the used heap exceeds a certain percentage of the maximum heap (75% by default)
 *<p />
 * The use of this store may require tuning JVM arguments for best effectiveness. An example of arguments that have
 * been shown to help on a Sun 1.6 JVM include "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC
 * -XX:CMSInitiatingOccupancyFraction=78 -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark
 * -XX:CMSMaxAbortablePrecleanTime=1000"
 */
public class CacheStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "cache";

    // default is start eviction when the heap exceeds 80%
    public static final int DEFAULT_EVICTING_HEAP_PERCENTAGE = 80;

    private final int evictionPercentage;

    public CacheStorageConfiguration() {
        evictionPercentage = DEFAULT_EVICTING_HEAP_PERCENTAGE;
    }

    @SuppressWarnings("unused")
    public CacheStorageConfiguration(VoldemortConfig config) {
        evictionPercentage = config.getCacheHeapEvictingPercentage();
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        ConcurrentMap<ByteArray, List<Versioned<byte[]>>> backingMap = ConcurrentLinkedHashMap.create(name,
                ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, evictionPercentage);
        return new InMemoryStorageEngine<ByteArray, byte[]>(name, backingMap);
    }

    public String getType() {
        return TYPE_NAME;
    }
}