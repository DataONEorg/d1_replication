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
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.configuration.ConversionException;
import org.apache.log4j.Logger;
import org.dataone.client.v1.CNode;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao.ReplicaDto;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v2.SystemMetadata;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

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

    private static Logger log = Logger.getLogger(StaleReplicationRequestAuditor.class);
    private static ReplicationService replicationService = ReplicationFactory
            .getReplicationService();
    private static final String STALE_REPLICATION_LOCK_NAME = "staleReplicationAuditingLock";
    private static HazelcastInstance hzClient = HazelcastClientFactory.getProcessingClient();

    @Override
    public void run() {
        if (ComponentActivationUtility.replicationIsActive()) {
            boolean isLocked = false;
            ILock lock = hzClient.getLock(STALE_REPLICATION_LOCK_NAME);
            try {
                isLocked = lock.tryLock();
                if (isLocked) {
                    log.debug("Stale Replication Request Auditor running.");
                    processStaleRequests();
                    log.debug("Stale Replication Request Auditor finished.");
                }
            } catch (Exception e) {
                log.error("Error processing stale requested replicas:", e);
            } finally {
                if (isLocked) {
                    lock.unlock();
                }
            }
        }
    }

    private void processStaleRequests() {
        CNode cn = getCNode();
        if (cn != null) {
            List<ReplicaDto> requestedReplicas = getReplicasToAudit();
            for (ReplicaDto result : requestedReplicas) {
                Identifier identifier = result.identifier;
                NodeReference nodeId = result.replica.getReplicaMemberNode();
                SystemMetadata sysmeta = null;
                try {
                    sysmeta = replicationService.getSystemMetadata(identifier);
                } catch (NotFound e) {
                    log.error("Cannot find system metadata for pid: " + identifier.getValue());
                    continue;
                }
                if (sysmeta == null) {
                    continue;
                }

                ReplicationCommunication rc = ReplicationCommunication.getInstance(nodeId);
                Checksum mnChecksum = null;
                try {
                    mnChecksum = rc.getChecksumFromMN(identifier, nodeId, sysmeta);
                } catch (BaseException e) {
                    log.warn(e.getMessage());
                }
                if (mnChecksum == null) {
                    deleteReplica(identifier, nodeId);
                } else {
                    updateReplicaToComplete(cn, identifier, nodeId, sysmeta);
                }
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
                    + " property correctly: " + ce.getMessage(), ce);
        }
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND, auditSecondsBeforeNow);
        Date auditDate = cal.getTime();
        return auditDate;
    }

    private List<ReplicaDto> getReplicasToAudit() {
        Date auditDate = calculateAuditDate();
        List<ReplicaDto> requestedReplicas = new ArrayList<ReplicaDto>();
        try {
            requestedReplicas = DaoFactory.getReplicationDao()
                    .getRequestedReplicasByDate(auditDate);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }
        log.debug("Stale Replication Request Auditor - pendingReplicasByDate size is "
                + requestedReplicas.size());
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

    private void updateReplicaToComplete(CNode cn, Identifier identifier, NodeReference nodeId,
            SystemMetadata sysmeta) {
        Replica replicaToUpdate = null;
        for (Replica replica : sysmeta.getReplicaList()) {
            if (replica.getReplicaMemberNode().getValue().equals(nodeId.getValue())) {
                replicaToUpdate = replica;
                break;
            }
        }
        if (replicaToUpdate != null) {
            log.debug("Stale Replication Request Auditor setting replica complete for pid: "
                    + identifier.getValue() + " for target mn: " + nodeId);
            replicationService.setReplicaToCompleted(identifier, nodeId);
        }
    }

    private void deleteReplica(Identifier identifier, NodeReference nodeRef) {
        replicationService.deleteReplicationMetadata(identifier, nodeRef);
    }
}
