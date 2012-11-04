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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.ConversionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.ReplicationDao.ReplicaDto;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.SystemMetadata;

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

            Date auditDate = calculateAuditDate();
            List<ReplicaDto> replicas = getReplicasToAudit(auditDate);
            log.debug("Stale queued replicas size: " + replicas.size());
            for (ReplicaDto replica : replicas) {
                if (replicationTaskQueue.containsTask(replica.replica.getReplicaMemberNode()
                        .getValue(), replica.identifier.getValue()) == false) {
                    log.debug("Stale queued replica is not in task queue, deleting. id: "
                            + replica.identifier.getValue() + " for node "
                            + replica.replica.getReplicaMemberNode().getValue());
                    deleteReplica(replica.identifier, replica.replica.getReplicaMemberNode());
                }
            }

            for (NodeReference nodeRef : replicationTaskQueue.getMemberNodesInQueue()) {
                int sizeOfQueue = replicationTaskQueue.getCountOfTasksForNode(nodeRef.getValue());
                log.debug("Queued tasks for member node: " + nodeRef.getValue() + " has: "
                        + sizeOfQueue + " tasks in queue.");
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

    private void deleteReplica(Identifier identifier, NodeReference nodeRef) {
        CNode cn = getCNode();
        if (cn != null) {
            try {
                SystemMetadata sysmeta = cn.getSystemMetadata(identifier);
                long serialVersion = sysmeta.getSerialVersion().longValue();
                cn.deleteReplicationMetadata(identifier, nodeRef, serialVersion);
            } catch (BaseException e) {
                log.error("Couldn't remove the replica entry for " + identifier.getValue() + " at "
                        + nodeRef.getValue() + ": " + e.getMessage());
                if (log.isDebugEnabled()) {
                    e.printStackTrace();

                }
            }
        }
    }

    private CNode getCNode() {
        CNode cn = null;
        try {
            cn = D1Client.getCN();
        } catch (BaseException e) {
            log.error("Couldn't connect to the CN to manage replica states: " + e.getMessage());
            if (log.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
        return cn;
    }

    private List<ReplicaDto> getReplicasToAudit(Date auditDate) {
        List<ReplicaDto> replicas = null;
        try {
            replicas = replicationDao.getQueuedReplicasByDate(auditDate);
        } catch (DataAccessException e) {
            log.error("ERROR attempting to get queued replicas to audit");
        }
        return replicas;
    }

    private Date calculateAuditDate() {
        int auditSecondsBeforeNow = -3600;
        try {
            auditSecondsBeforeNow = Settings.getConfiguration().getInt(
                    "replication.audit.pending.window");
        } catch (ConversionException ce) {
            log.error("Couldn't convert the replication.audit.pending.window"
                    + " property correctly: " + ce.getMessage(), ce);
        }
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, auditSecondsBeforeNow);
        Date auditDate = cal.getTime();
        return auditDate;
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
