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
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.utils.ByteBufferBackedInputStream;

public abstract class AbstractClientRequest<T> implements ClientRequest<T> {

    protected T result;

    protected VoldemortException error;

    protected final Logger logger = Logger.getLogger(getClass());

    protected abstract T readInternal(DataInputStream inputStream) throws IOException;

    public void setServerError(Exception e) {
        if(e instanceof VoldemortException)
            error = (VoldemortException) e;
        else
            error = new VoldemortException(e);
    }

    public void completed() {

    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        try {
            try {
                readInternal(new DataInputStream(new ByteBufferBackedInputStream(buffer)));
            } catch(VoldemortException e) {
                // Ignore application-level exceptions...
            }

            return !buffer.hasRemaining();
        } catch(Exception e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isDebugEnabled())
                logger.debug("Probable partial read occurred causing exception", e);

            return false;
        }
    }

    public void read(DataInputStream inputStream) throws IOException {
        try {
            result = readInternal(inputStream);
        } catch(VoldemortException e) {
            error = e;
        }
    }

    public T getResult() {
        if(error != null)
            throw error;

        return result;
    }

}
