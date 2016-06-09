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
package org.dataone.service.cn.replication;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.ReplicationDao.ReplicaDto;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

/**
 * A member node replication work queue.
 * 
 * Provides an abstraction for working with queued replication objects which
 * represent a replication that needs to be performed. These tasks are
 * represented by replica objects in the 'QUEUED' status.
 * 
 * Essentially this is a Queued Replica Work Queue -- its job is to move
 * replicas to the requested status.
 * 
 * @author sroseboo
 * 
 */
public class ReplicationTaskQueue {

    private static Logger log = Logger.getLogger(ReplicationTaskQueue.class);

    private ReplicationDao replicationDao;
    private ReplicationService replicationService;
    private HazelcastInstance hzClient;

    public ReplicationTaskQueue() {
        replicationDao = DaoFactory.getReplicationDao();
        replicationService = ReplicationFactory.getReplicationService();
        hzClient = HazelcastClientFactory.getProcessingClient();
    }

    public void logState() {
        if (log.isDebugEnabled()) {
            log.debug("logging replication task queue state:");
            for (NodeReference nodeReference : getMemberNodesInQueue()) {
                log.debug("Member Node: " + nodeReference.getValue() + " has "
                        + getCountOfTasksForNode(nodeReference.getValue()));
            }
            log.debug("finished reporting replication task queue state");
        }
    }

    /**
     * Returns a Collection of NodeReference objects for member nodes that
     * currently have queued replicas.
     * 
     * @return
     */
    public Collection<NodeReference> getMemberNodesInQueue() {
        Collection<NodeReference> nodes = new ArrayList<NodeReference>();
        try {
            nodes = replicationDao.getMemberNodesWithQueuedReplica();
        } catch (DataAccessException dae) {
            log.error("Cannot get member nodes in queue.", dae);
        }
        return nodes;
    }

    /**
     * Returns the count of queued replica objects for the node specified by the
     * nodeId parameter.
     * 
     * @param nodeId
     * @return
     */
    public int getCountOfTasksForNode(String nodeId) {
        int count = 0;
        try {
            count = replicationDao.getQueuedReplicaCountByNode(nodeId);
        } catch (DataAccessException dae) {
            log.error("Cannot get count of tasks for node: " + nodeId, dae);
        }
        return count;
    }

    /**
     * Indicates if a queued replica currently exists for the specified
     * identifier and the member node specified by the nodeId parameter.
     * 
     * @param nodeId
     * @param identifier
     * @return
     */
    private boolean containsTask(String nodeId, String identifier) {
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

    /**
     * Attempt to move each 'queued' replica to the 'requested' state for the
     * member node specified by the memberNodeIdentifierValue parameter.
     * 
     * @param memberNodeIdentifierValue
     */
    public void processAllTasksForMN(String memberNodeIdentifierValue) {
        String mnId = memberNodeIdentifierValue;
        boolean isLocked = false;

        if (mnId != null) {
            log.debug("ReplicationTaskQueue. Processing all tasks for node: " + mnId + ".");
            Collection<ReplicaDto> queuedReplicas = getQueuedReplicas(mnId);
            int queuedCount = queuedReplicas.size();
            log.debug(queuedCount + " tasks for mn: " + mnId);
            if (queuedCount > 0) {
                // LOCK THIS MEMBER NODE FOR PROCESSING
                ILock lock = hzClient.getLock(memberNodeIdentifierValue);
                try {
                    isLocked = lock.tryLock();

                    if (isLocked) {
                        for (ReplicaDto replica : queuedReplicas) {
                            if (replica != null) {
                                try {
                                    this.requestReplication(replica.identifier,
                                            replica.replica.getReplicaMemberNode());
                                } catch (Exception e) {
                                    log.error("Caught exception requesting replica", e);
                                }
                            }
                        }
                    } else {
                        log.warn("Didn't get the lock for node id " + memberNodeIdentifierValue);
                    }
                } catch (Exception e) {
                    log.error("Error requesting replica for queued replica", e);
                } finally {
                    // UNLOCK MEMBER NODE FOR PROCESSING
                    if (isLocked) {
                        lock.unlock();
                    }
                }
            }
        }
    }

    /**
     * Encapsulates implementation details on how a queued replica is moved to
     * the requested state. Sets a replica to Queued status, which effectively
     * removes it from the QueuedTaskQueue
     * 
     */
    private void requestReplication(Identifier identifier, NodeReference targetNode) {
        if (identifier == null || targetNode == null) {
            return;
        }
        log.debug("Requesting replica for id " + identifier.getValue() + " and target node "
                + targetNode.getValue());
        try {
            replicationService.requestQueuedReplication(identifier, targetNode);
        } catch (Exception e) {
            log.error("Error requesting replica", e);
        }
    }

    /**
     * Represents the task or work for the member node specified by the mnId
     * parameter. In terms of replication, the work here represents replicas
     * which have been queued for replication, waiting to be transition to
     * requested state.
     * 
     * @param mnId
     * @return
     */
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
