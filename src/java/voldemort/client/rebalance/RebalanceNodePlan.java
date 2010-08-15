/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.client.rebalance;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.List;

/**
 * Plan for rebalancing an individual stealer node
 */
public class RebalanceNodePlan {

    private final int stealerNode;
    private final List<RebalancePartitionsInfo> rebalanceTaskList;

    public RebalanceNodePlan(int stealerNode, List<RebalancePartitionsInfo> rebalanceTaskList) {
        this.stealerNode = stealerNode;
        this.rebalanceTaskList = rebalanceTaskList;
    }

    public int getStealerNode() {
        return stealerNode;
    }

    public List<RebalancePartitionsInfo> getRebalanceTaskList() {
        return rebalanceTaskList;
    }

    /**
     * Divide the <code>RebalanceNodePlan</code> into a {@link Multimap} of tasks per donor
     *
     * @return The {@link Multimap}
     */
    public Multimap<Integer, RebalancePartitionsInfo> tasksByDonor() {
        Multimap<Integer, RebalancePartitionsInfo> plan = HashMultimap.create();
        for(RebalancePartitionsInfo subTask: rebalanceTaskList)
            plan.put(subTask.getDonorId(), subTask);
        return plan;
    }
}
