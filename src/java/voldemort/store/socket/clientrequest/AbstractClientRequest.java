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

package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.IOException;

import voldemort.VoldemortException;

/**
 * AbstractClientRequest implements ClientRequest to provide some basic
 * mechanisms that most implementations will need.
 * 
 * @param <T> Return type
 */

public abstract class AbstractClientRequest<T> implements ClientRequest<T> {

    private T result;

    private VoldemortException error;

    private volatile boolean isCompleted = false;

    protected abstract T parseResponseInternal(DataInputStream inputStream) throws IOException;

    public void setServerError(Exception e) {
        if(e instanceof VoldemortException)
            error = (VoldemortException) e;
        else
            error = new VoldemortException(e);
    }

    public final void completed() {
        isCompleted = true;
    }

    public void parseResponse(DataInputStream inputStream) throws IOException {
        try {
            result = parseResponseInternal(inputStream);
        } catch(VoldemortException e) {
            error = e;
        }
    }

    public T getResult() {
        if(!isCompleted)
            throw new IllegalStateException("Client response not complete, cannot determine result");

        if(error != null)
            throw error;

        return result;
    }

}
