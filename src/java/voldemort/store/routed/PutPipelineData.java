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

import com.google.common.collect.Sets;
import voldemort.cluster.Node;
import voldemort.store.routed.action.PerformHintedHandoff;
import voldemort.store.routed.action.PerformSerialPutRequests;
import voldemort.versioning.Versioned;

import java.util.Collection;
import java.util.Set;

/**
 * This is used only by the "put" operation as it includes data specific only to
 * that operation.
 */

public class PutPipelineData extends BasicPipelineData<Void> {

    private Node master;

    private Versioned<byte[]> versionedCopy;

    private final boolean enableHintedHandoff;

    private final Set<Integer> failedNodes = Sets.newHashSet();

    private final String storeName;
    
    /**
     * Creates pipeline data for a put operation.
     *
     * @param enableHintedHandoff Enable hinted handoff
     */
    
    public PutPipelineData(boolean enableHintedHandoff, String storeName) {
        super();
        this.enableHintedHandoff = enableHintedHandoff;
        this.storeName = storeName;
    }

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

    /**
     * Registers the node if of a failed node. Used by {@link PerformHintedHandoff}.
     *
     * @param nodeId
     */

    public void addFailedNode(int nodeId) {
        failedNodes.add(nodeId);
    }

    /**
     * List all the failed nodes.
     *
     * @return Ids of failed nodes
     */

    public Collection<Integer> getFailedNodes() {
        return failedNodes;
    }

    /**
     * Is hinted handoff enabled?
     *
     * @return True if hinted handoff is enabled
     */
    
    public boolean isHintedHandoffEnabled() {
        return enableHintedHandoff;
    }

    public String getStoreName() {
        return storeName;
    }
}
