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
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.NodeReference;

public class QueuedReplicationAuditor implements Runnable {

    private static Log log = LogFactory.getLog(QueuedReplicationAuditor.class);

    private ReplicationTaskQueue replicationTaskQueue = new ReplicationTaskQueue();

    private int requestLimit = Settings.getConfiguration().getInt(
            "replication.concurrent.request.limit");

    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();

    public QueuedReplicationAuditor() {
    }

    @Override
    public void run() {
        if (ReplicationUtil.replicationIsActive()) {
            log.debug("Queued Request Auditor running.");
            for (NodeReference nodeRef : replicationTaskQueue.getMemberNodesInQueue()) {
                int sizeOfQueue = replicationTaskQueue.getCountOfTasksForNode(nodeRef.getValue());
                if (sizeOfQueue > 0) {
                    int sizeOfRequested = getRequestedCount(nodeRef);
                    if (sizeOfRequested > -1) {
                        if (sizeOfRequested == 0
                                || (requestLimit > (sizeOfQueue + sizeOfRequested))) {
                            replicationTaskQueue.processAllTasksForMN(nodeRef.getValue());
                        }
                    }
                }
            }
            log.debug("Queued Replication Auditor finished.");
        }
    }

    private int getRequestedCount(NodeReference nodeRef) {
        int sizeOfRequested = -1;
        try {
            sizeOfRequested = replicationDao.getRequestedReplicationCount(nodeRef);
        } catch (DataAccessException e) {
            log.error("Unable to get oustanding rplication count for mn: " + nodeRef.getValue(), e);
        }
        return sizeOfRequested;
    }
}
