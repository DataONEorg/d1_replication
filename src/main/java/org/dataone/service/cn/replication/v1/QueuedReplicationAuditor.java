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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.NodeReference;

public class QueuedReplicationAuditor implements Runnable {

    private static Log log = LogFactory.getLog(QueuedReplicationAuditor.class);

    private ReplicationTaskQueue replicationTaskQueue = new ReplicationTaskQueue();

    public QueuedReplicationAuditor() {
    }

    @Override
    public void run() {
        if (ReplicationUtil.replicationIsActive()) {
            log.debug("Queued Request Auditor running.");
            runQueuedTasks();
            log.debug("Queued Replication Auditor finished.");
        }
    }

    private void runQueuedTasks() {
        for (NodeReference nodeRef : replicationTaskQueue.getMemberNodesInQueue()) {
            int sizeOfQueue = replicationTaskQueue.getCountOfTasksForNode(nodeRef.getValue());
            log.debug("Queued tasks for member node: " + nodeRef.getValue() + " has: "
                    + sizeOfQueue + " tasks in queue.");
            if (sizeOfQueue > 0) {
                replicationTaskQueue.processAllTasksForMN(nodeRef.getValue());
            }
        }
    }
}
