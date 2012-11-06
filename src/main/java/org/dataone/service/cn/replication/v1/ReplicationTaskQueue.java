/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
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
 * 
 */
package org.dataone.service.cn.replication.v1;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.ReplicationDao.ReplicaDto;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;

/**
 * Abstract member node replication task work queue. Provides interface for
 * registering an entry listener, adding tasks, getting tasks. Encapsulates
 * entry listening strategies.
 * 
 * @author sroseboo
 * 
 */
public class ReplicationTaskQueue {

    private static Log log = LogFactory.getLog(ReplicationTaskQueue.class);
    private static ReplicationDao replicationDao = DaoFactory.getReplicationDao();
    private static ReplicationService replicationService = new ReplicationService();

    public ReplicationTaskQueue() {
    }

    public void logState() {
        log.debug("logging replication task queue state:");
        for (NodeReference nodeReference : getMemberNodesInQueue()) {
            log.debug("Member Node: " + nodeReference.getValue() + " has "
                    + getCountOfTasksForNode(nodeReference.getValue()));
        }
        log.debug("finished reporting replication task queue state");
    }

    public Collection<NodeReference> getMemberNodesInQueue() {
        Collection<NodeReference> nodes = new ArrayList<NodeReference>();
        try {
            nodes = replicationDao.getMemberNodesWithQueuedReplica();
        } catch (DataAccessException dae) {
            log.error("Cannot get member nodes in queue.", dae);
        }
        return nodes;
    }

    public int getCountOfTasksForNode(String nodeId) {
        int count = 0;
        try {
            count = replicationDao.getQueuedReplicaCountByNode(nodeId);
        } catch (DataAccessException dae) {
            log.error("Cannot get count of tasks for node: " + nodeId, dae);
        }
        return count;
    }

    public boolean containsTask(String nodeId, String identifier) {
        log.debug("invoking contains task");
        if (nodeId == null || identifier == null) {
            return false;
        }
        boolean contains = false;
        try {
            contains = replicationDao.queuedReplicaExists(identifier, nodeId);
        } catch (DataAccessException dae) {
            log.error("Error executing queuedReplicaExists", dae);
        }
        return contains;
    }

    public void processAllTasksForMN(String memberNodeIdentifierValue) {
        String mnId = memberNodeIdentifierValue;
        if (mnId != null) {
            log.debug("ReplicationTaskQueue. Processing all tasks for node: " + mnId + ".");
            Collection<ReplicaDto> queuedReplicas = getQueuedReplicas(mnId);
            int queuedCount = queuedReplicas.size();
            if (queuedCount > 0) {
                log.debug(queuedCount + " tasks for mn: " + mnId);
                // TODO: LOCK THIS MEMBER NODE FOR PROCESSING
                try {

                    for (ReplicaDto replica : queuedReplicas) {
                        if (replica != null) {
                            try {
                                this.executeTask(replica.identifier,
                                        replica.replica.getReplicaMemberNode());
                            } catch (Exception e) {
                                log.error("Caught exception requesting replica", e);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Error requesting replica for queued replica", e);
                } finally {
                    // TODO: UNLOCK MEMBER NODE FOR PROCESSING
                }
            }
        }
    }

    /**
     * Encapsulates implementation details on how a replication task is
     * executed.
     * 
     * @param task
     */
    private void executeTask(Identifier identifier, NodeReference targetNode) {
        if (identifier == null || targetNode == null) {
            return;
        }
        log.debug("Requesting replica for id " + identifier.getValue() + " and target node "
                + targetNode.getValue());
        try {
            replicationService.requestReplica(identifier, targetNode);
        } catch (Exception e) {
            log.error("Error requesting replica", e);
        }
    }

    private Collection<ReplicaDto> getQueuedReplicas(String mnId) {
        Collection<ReplicaDto> queuedReplicas = new ArrayList<ReplicaDto>();
        try {
            queuedReplicas = replicationDao.getQueuedReplicasByNode(mnId);
        } catch (DataAccessException dae) {
            log.error("unable to get queue replicas for node: " + mnId, dae);
        }
        return queuedReplicas;
    }

}
