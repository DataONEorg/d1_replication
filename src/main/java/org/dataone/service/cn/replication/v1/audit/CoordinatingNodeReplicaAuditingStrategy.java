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
import org.dataone.client.CNode;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.cn.replication.v1.ReplicationFactory;
import org.dataone.service.cn.replication.v1.ReplicationService;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;

import com.hazelcast.core.IMap;

public class CoordinatingNodeReplicaAuditingStrategy {

    public static Log log = LogFactory.getLog(MemberNodeReplicaAuditingStrategy.class);

    // pass/inject mnMap from higher layer....a service/resource...
    private Map<NodeReference, CNode> cnMap = new HashMap<NodeReference, CNode>();

    private ReplicationService replicationService;
    private IMap<NodeReference, Node> hzNodes;

    public CoordinatingNodeReplicaAuditingStrategy() {
        replicationService = ReplicationFactory.getReplicationService();
        hzNodes = HazelcastClientFactory.getProcessingClient().getMap("hzNodes");
    }

    public void auditPids(List<Identifier> pids, Date auditDate) {
        for (Identifier pid : pids) {
            this.auditPid(pid, auditDate);
        }
    }

    public void auditPid(Identifier pid, Date auditDate) {

        SystemMetadata sysMeta = null;
        try {
            sysMeta = replicationService.getSystemMetadata(pid);
        } catch (NotFound e) {

        }
        if (sysMeta == null) {
            log.error("Cannot get system metadata from CN for pid: " + pid
                    + ".  Could not audit CN replica for pid: " + pid + "");
            return;
        }

        for (Replica replica : sysMeta.getReplicaList()) {
            if (isCNodeReplica(replica)) {
                auditCNodeReplica(sysMeta, replica);
            } else {
                log.error("found MN replica in Coordinating Node auditing for pid: "
                        + pid.getValue());
            }
        }
        return;
    }

    private void auditCNodeReplica(SystemMetadata sysMeta, Replica replica) {

        //TODO: need to do this for each CN - how to get list of CN?
        Checksum expected = sysMeta.getChecksum();
        Checksum actual = null;
        boolean valid = true;
        try {
            actual = replicationService.calculateCNChecksum(sysMeta.getIdentifier());
        } catch (NotFound e) {
            valid = false;
        }
        if (actual != null && valid) {
            valid = ChecksumUtil.areChecksumsEqual(expected, actual);
        }
        if (valid) {
            updateReplicaVerified(sysMeta.getIdentifier(), replica);
        } else {
            //TODO: how to handle invalid CN relica?
        }
    }

    private boolean isCNodeReplica(Replica replica) {
        return NodeType.CN.equals(hzNodes.get(replica.getReplicaMemberNode()).getType());
    }

    private void updateReplicaVerified(Identifier pid, Replica replica) {
        replica.setReplicaVerified(calculateReplicaVerifiedDate());
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica verified date  for pid: " + pid + " on CN");
        }
    }

    private Date calculateReplicaVerifiedDate() {
        return new Date(System.currentTimeMillis());
    }
}
