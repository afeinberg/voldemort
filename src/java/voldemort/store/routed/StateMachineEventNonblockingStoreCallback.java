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

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.StateMachine.Event;
import voldemort.utils.ByteArray;

public class StateMachineEventNonblockingStoreCallback implements NonblockingStoreCallback {

    private final StateMachine stateMachine;

    private final Node node;

    private final ByteArray key;

    public StateMachineEventNonblockingStoreCallback(StateMachine stateMachine,
                                                     Node node,
                                                     ByteArray key) {
        this.stateMachine = stateMachine;
        this.node = node;
        this.key = key;
    }

    public void requestComplete(Object result, long requestTime) throws VoldemortException {
        RequestCompletedCallback requestCompletedCallback = new RequestCompletedCallback(node,
                                                                                         key,
                                                                                         requestTime,
                                                                                         result);

        stateMachine.addEvent(Event.RESPONSE_RECEIVED, requestCompletedCallback);
    }

}
