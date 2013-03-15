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
package org.dataone.service.cn.replication.v1.audit;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.MNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.cn.replication.v1.ReplicationFactory;
import org.dataone.service.cn.replication.v1.ReplicationManager;
import org.dataone.service.cn.replication.v1.ReplicationService;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.core.IMap;

/**
 * A single audit task to be queued and executed by the Replication Service. The
 * audit task is generated from the result of a query on objects with replicas
 * that haven't been verified in 2 or more months.
 * 
 * This type of audit verifies both that the replication policy is fufilled and
 * that each replica is still valid (by comparing checksum values).
 * 
 * Replicas found with invalid checksums have system metadata replica status
 * updated to INVALID. Pids with unfufilled replica policies are sent to
 * ReplicationManager.
 * 
 * Verified replicas have their verified date updated to reflect audit complete
 * date.
 * 
 * Reuses ReplicationService to perform common operations.
 * 
 * @author sroseboo
 * 
 */
public class MemberNodeReplicaAuditingStrategy {

    public static Log log = LogFactory.getLog(MemberNodeReplicaAuditingStrategy.class);

    private Map<NodeReference, MNode> mnMap = new HashMap<NodeReference, MNode>();

    private ReplicationManager replicationManager;
    private ReplicationService replicationService;
    private IMap<NodeReference, Node> hzNodes;

    public MemberNodeReplicaAuditingStrategy() {
        replicationService = ReplicationFactory.getReplicationService();
        replicationManager = ReplicationFactory.getReplicationManager();
        hzNodes = HazelcastClientFactory.getProcessingClient().getMap("hzNodes");
    }

    public boolean auditPids(List<Identifier> pids) {
        for (Identifier pid : pids) {
            this.auditPid(pid);
        }
        return true;
    }

    public boolean auditPid(Identifier pid) {
        boolean queueToReplication = false;
        SystemMetadata sysMeta = replicationService.getSystemMetadata(pid);
        if (sysMeta == null) {
            log.error("Cannot get system metadata from CN.  Could not audit replication.");
            return false;
        }
        int validReplicaCount = 0;
        String cnChecksumValue = sysMeta.getChecksum().getValue();

        for (Replica replica : sysMeta.getReplicaList()) {
            // Replicas on CN node do not count towards total replica count.
            if (isCNode(replica)) {
                // TODO: any processing for CN replicas?
                // ask CN to calculate checksum, compare to system metadata?
                continue;
            } else {
                MNode mn = getMNode(replica.getReplicaMemberNode());
                if (mn == null) {
                    log.error("Cannot get MN: " + replica.getReplicaMemberNode().getValue()
                            + " unable to verify replica information.");
                    // TODO: how to handle not finding the MN? is the
                    // replica
                    // INVALID? MN down for maintenance, temporary outage?
                    continue;
                }
                Checksum mnChecksum = getChecksumFromMN(pid, sysMeta, mn);
                if (mnChecksum == null) {
                    log.error("Cannot get checksum for pid: " + pid + " from MN: "
                            + replica.getReplicaMemberNode().getValue());
                    handleInvalidReplica(pid, replica);
                    queueToReplication = true;
                } else if (mnChecksum.getValue().equals(cnChecksumValue)) {
                    validReplicaCount++;
                    replica.setReplicaVerified(calculateReplicaVerifiedDate());
                    boolean success = replicationService.updateReplicationMetadata(pid, replica);
                    if (!success) {
                        log.error("Cannot update replica verified date  for pid: " + pid + " on CN");
                        queueToReplication = true;
                    }
                } else {
                    log.error("Checksum mismatch for pid: " + pid + " against MN: "
                            + replica.getReplicaMemberNode() + " CN checksum: " + cnChecksumValue
                            + " MN checksum: " + mnChecksum.getValue());
                    handleInvalidReplica(pid, replica);
                    queueToReplication = true;
                }
                if (queueToReplication) {
                    break;
                }
            }
        }
        if (shouldSendToReplication(queueToReplication, sysMeta, validReplicaCount)) {
            boolean success = sendToReplication(pid);
            if (!success) {
                log.error("Cannot queue pid: " + pid + " to replication queue.");
                return false;
            }
        }
        return true;
    }

    private Date calculateReplicaVerifiedDate() {
        // TODO: should be current time with no timezone...
        return new Date(System.currentTimeMillis());
    }

    private boolean isCNode(Replica replica) {
        return NodeType.CN.equals(hzNodes.get(replica.getReplicaMemberNode()).getType());
    }

    private boolean shouldSendToReplication(boolean queueToReplication, SystemMetadata sysMeta,
            int validReplicaCount) {
        return queueToReplication
                || validReplicaCount != sysMeta.getReplicationPolicy().getNumberReplicas()
                        .intValue();
    }

    private void handleInvalidReplica(Identifier pid, Replica replica) {
        replica.setReplicationStatus(ReplicationStatus.INVALIDATED);
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica status to INVALID for pid: " + pid + " on MN: "
                    + replica.getReplicaMemberNode().getValue());
        }
    }

    private boolean sendToReplication(Identifier pid) {
        try {
            replicationManager.createAndQueueTasks(pid);
        } catch (Exception e) {
            log.error("Pid: " + pid + " not accepted by replicationManager createAndQueueTasks: ",
                    e);
            return false;
        }
        return true;
    }

    private Checksum getChecksumFromMN(Identifier pid, SystemMetadata sysMeta, MNode mn) {
        try {
            return mn.getChecksum(pid, sysMeta.getChecksum().getAlgorithm());
        } catch (BaseException be) {
            log.error(
                    "Unable to get checksum from mn: " + mn.getNodeId() + " for id: "
                            + pid.getValue(), be);
        }
        return null;
    }

    private MNode getMNode(NodeReference nodeRef) {
        if (!mnMap.containsKey(nodeRef)) {
            MNode mn = replicationService.getMemberNode(nodeRef);
            if (mn != null) {
                mnMap.put(nodeRef, mn);
            }
        }
        return mnMap.get(nodeRef);
    }
}
