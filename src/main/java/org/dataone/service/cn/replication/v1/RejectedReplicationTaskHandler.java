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

package org.dataone.service.cn.replication.v1;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

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

    /* the instance of the Hazelcast processing cluster member */
    private HazelcastInstance hzMember;

    /* The name of the replication tasks queue */
    private String tasksQueue;

    /* The Hazelcast distributed replication tasks queue*/
    private IQueue<MNReplicationTask> replicationTasks;

    /**
     * Constructor to create a rejected replication task handler instance
     */
    public RejectedReplicationTaskHandler() {
        
        // get the default cluster instance and a replication task queue reference
        this.tasksQueue = 
            Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
        this.hzMember = Hazelcast.getDefaultInstance();
        this.replicationTasks = this.hzMember.getQueue(tasksQueue);

    }
    
    /**
     * A handler method that adds rejected replication tasks back into the
     * Hazelcast replication task queue to be completed later or by another 
     * coordinating node that may pick it up from the queue.
     */
    public void rejectedExecution(Runnable replicationTask, ThreadPoolExecutor executor) {

        MNReplicationTask task = (MNReplicationTask) replicationTask;
        
        String msg = "Replication task id " + task.getTaskid() +
            " for identifier " + task.getPid().getValue() + " was rejected.";
        log.warn(msg);
        
        try {
            // add it back to the queue for re-execution by one of the
            // coordinating nodes
            this.replicationTasks.add(task);
            log.info("Added task id" + task.getTaskid() + " for identifier " + 
                task.getPid().getValue() + " back to the replication task queue.");

        } catch (RuntimeException e) {
            String message = "Failed to add task id " + task.getTaskid() +
                " back to the replication task queue for identifier " +
                task.getPid().getValue() + ". The error message was " +
                e.getCause().getMessage();
            log.error(message);
            
        }
        
    }

}
