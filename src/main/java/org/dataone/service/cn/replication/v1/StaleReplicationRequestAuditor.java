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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConversionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao.ReplicaResult;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * Auditor class to inspect Member Node replication requests that have been in
 * request status for longer than a specific period of time (default 1 hour).
 * Checksum is called on the member node to test whether the replication request
 * has actually completed (and missed by the CN). If the target member node can
 * supply the checksum then the replication request is update to be completed.
 * 
 * @author sroseboo
 * 
 */
public class StaleReplicationRequestAuditor implements Runnable {

    private static Log log = LogFactory.getLog(StaleReplicationRequestAuditor.class);

    @Override
    public void run() {

        Date auditDate = calculateAuditDate();
        List<ReplicaResult> requestedReplicas = getReplicasToAudit(auditDate);
        CNode cn = getCNode();
        if (cn != null) {
            Map<String, MNode> memberNodes = new HashMap<String, MNode>();
            for (ReplicaResult result : requestedReplicas) {
                Identifier identifier = result.identifier;
                NodeReference nodeId = result.memberNode;
                SystemMetadata sysmeta = getSystemMetadata(cn, identifier);
                if (sysmeta == null) {
                    continue;
                }
                MNode mn = getMemberNode(memberNodes, nodeId);
                if (mn == null) {
                    continue;
                }
                Checksum mnChecksum = getChecksumFromMN(identifier, nodeId, sysmeta, mn);
                if (mnChecksum == null) {
                    continue;
                }
                updateReplicaToComplete(cn, identifier, nodeId, sysmeta);
            }
        }
    }

    private Date calculateAuditDate() {
        int auditSecondsBeforeNow = -3600;
        try {
            auditSecondsBeforeNow = Settings.getConfiguration().getInt(
                    "replication.audit.pending.window");
        } catch (ConversionException ce) {
            log.error("Couldn't convert the replication.audit.pending.window"
                    + " property correctly: " + ce.getMessage());
        }
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, auditSecondsBeforeNow);
        Date auditDate = cal.getTime();
        return auditDate;
    }

    private List<ReplicaResult> getReplicasToAudit(Date auditDate) {
        List<ReplicaResult> requestedReplicas = new ArrayList<ReplicaResult>();
        try {
            requestedReplicas = DaoFactory.getReplicationDao()
                    .getRequestedReplicasByDate(auditDate);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
        log.debug("pendingReplicasByDate size is " + requestedReplicas.size());
        return requestedReplicas;
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

    private SystemMetadata getSystemMetadata(CNode cn, Identifier identifier) {
        SystemMetadata sysmeta = null;
        try {
            sysmeta = cn.getSystemMetadata(identifier);
        } catch (BaseException be) {
            log.error("Stale Replica Status: cannot get system metadata from CN for id: "
                    + identifier.getValue());
        }
        return sysmeta;
    }

    private MNode getMemberNode(Map<String, MNode> memberNodes, NodeReference nodeId) {
        MNode mn = null;
        if (memberNodes.containsKey(nodeId.getValue())) {
            mn = memberNodes.get(nodeId.getValue());
        } else {
            try {
                mn = D1Client.getMN(nodeId);
            } catch (BaseException e) {
                log.error("Couldn't connect to the MN to manage replica states: " + e.getMessage());
                if (log.isDebugEnabled()) {
                    e.printStackTrace();
                }
            }
            if (mn != null) {
                memberNodes.put(nodeId.getValue(), mn);
            }
        }
        return mn;
    }

    private Checksum getChecksumFromMN(Identifier identifier, NodeReference nodeId,
            SystemMetadata sysmeta, MNode mn) {
        Checksum mnChecksum = null;
        try {
            mnChecksum = mn.getChecksum(identifier, sysmeta.getChecksum().getAlgorithm());
        } catch (BaseException e) {
            log.debug("Stale Replica Status Audit: Cannot get checksum from MN: "
                    + nodeId.getValue() + " for pid: " + identifier.getValue());
        }
        return mnChecksum;
    }

    private void updateReplicaToComplete(CNode cn, Identifier identifier, NodeReference nodeId,
            SystemMetadata sysmeta) {
        try {
            Replica replicaToUpdate = null;
            for (Replica replica : sysmeta.getReplicaList()) {
                if (replica.getReplicaMemberNode().getValue().equals(nodeId.getValue())) {
                    replicaToUpdate = replica;
                    break;
                }
            }
            if (replicaToUpdate != null) {
                replicaToUpdate.setReplicationStatus(ReplicationStatus.COMPLETED);
                long serialVersion = sysmeta.getSerialVersion().longValue();
                cn.updateReplicationMetadata(identifier, replicaToUpdate, serialVersion);
            }
        } catch (BaseException e) {
            log.error("Stale Replica Audit - cannot update replica for pid: "
                    + identifier.getValue());
        }
    }
}
