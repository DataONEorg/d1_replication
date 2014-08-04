/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dataone.service.cn.replication;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.service.exceptions.BaseException;

/**
 * A class used to handle rejected replication tasks that were submitted to the 
 * executor service but failed to be executed due to resource constraints or other
 * issues (like executor shutdown).
 * 
 * @author cjones
 *
 */
public class RejectedReplicationTaskHandler implements RejectedExecutionHandler {

    /* Get a Log instance */
    public static Log log = LogFactory.getLog(RejectedReplicationTaskHandler.class);

    /* a reference to the coordinating node */
    private CNode cn;
    
    /**
     * Constructor to create a rejected replication task handler instance
     */
    public RejectedReplicationTaskHandler() {
        
    }
    
    /**
     * A handler method that adds rejected replication tasks back into the
     * Hazelcast replication task queue to be completed later or by another 
     * coordinating node that may pick it up from the queue.
     */
    public void rejectedExecution(Runnable replicationTask, 
            ThreadPoolExecutor executor) {

        boolean deleted = false;

        MNReplicationTask task = (MNReplicationTask) replicationTask;
        
        String msg = "Replication task id " + task.getTaskid() +
            " for identifier " + task.getPid().getValue() + " was rejected.";
        log.warn(msg);
        
        try {
            this.cn = D1Client.getCN();
            long serialVersion = this.cn.getSystemMetadata(null,
                    task.getPid()).getSerialVersion().longValue();
            deleted = this.cn.deleteReplicationMetadata(null,
                    task.getPid(), task.getTargetNode(), serialVersion);
            log.info("Deleted replica entry for" + 
                task.getTargetNode().getValue() + " and identifier " + 
                task.getPid().getValue() + " from the replica list.");
            
        } catch (BaseException e) {
            log.error("Unable to delete the replica entry for identifier " +
                task.getPid().getValue() +
                ": " + e.getMessage());
            if ( log.isDebugEnabled() ) {
                e.printStackTrace();
                
            }
        }
        
    }

}
