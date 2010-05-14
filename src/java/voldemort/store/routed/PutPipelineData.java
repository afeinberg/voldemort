/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This is used only by the "put" operation as it includes data specific only to
 * that operation.
 */

public class PutPipelineData extends BasicPipelineData<Void> {

    private Cluster cluster;

    private Node master;

    private Versioned<byte[]> versionedCopy;

    private Queue<Integer> slopQueue = new ConcurrentLinkedQueue<Integer>();

    private Map<Node, Store<ByteArray, Slop>> slopStores;

    private boolean enableHintedHandoff;


    /**
     * Returns the previously determined "master" node. This is the first node
     * in the preference list that succeeded in "putting" the value.
     * 
     * @return Master {@link Node}, or null if not yet assigned
     */

    public Node getMaster() {
        return master;
    }

    /**
     * Assigns the "master" {@link Node} as determined by
     * {@link PerformSerialPutRequests}. This is the first node in the
     * preference list that "put" the value successfully.
     * 
     * @param master "Master" {@link Node}
     */

    public void setMaster(Node master) {
        this.master = master;
    }

    /**
     * Returns the copy of the {@link Versioned} as determined by
     * {@link PerformSerialPutRequests}.
     * 
     * @return {@link Versioned} copy
     */

    public Versioned<byte[]> getVersionedCopy() {
        return versionedCopy;
    }

    /**
     * The copy of the {@link Versioned} instance that was incremented before
     * attempting to put on the remote Voldemort node.
     * 
     * @param versionedCopy
     */

    public void setVersionedCopy(Versioned<byte[]> versionedCopy) {
        this.versionedCopy = versionedCopy;
    }

    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    public void setEnableHintedHandoff(boolean enableHintedHandoff) {
        this.enableHintedHandoff = enableHintedHandoff;
    }

    public void addSlop(int nodeId) {
        slopQueue.offer(nodeId);
    }

    public Queue<Integer> getSlopQueue() {
        return slopQueue;
    }

    public void setSlopStores(Map<Node, Store<ByteArray, Slop>> slopStores) {
        this.slopStores = slopStores;
    }

    public Map<Node, Store<ByteArray, Slop>> getSlopStores() {
        return slopStores;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public Cluster getCluster() {
        return cluster;
    }
}
