package org.dataone.service.cn.replication.v1;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

public class StaleReplicationRequestAuditor implements Runnable {

    private static Log log = LogFactory.getLog(StaleReplicationRequestAuditor.class);

    @Override
    public void run() {

        List<ReplicaResult> requestedReplicas = new ArrayList<ReplicaResult>();

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
        try {
            requestedReplicas = DaoFactory.getReplicationDao()
                    .getRequestedReplicasByDate(auditDate);
        } catch (DataAccessException e) {
            e.printStackTrace();
        }

        log.debug("pendingReplicasByDate size is " + requestedReplicas.size());

        CNode cn = null;
        try {
            cn = D1Client.getCN();
        } catch (BaseException e) {
            log.error("Couldn't connect to the CN to manage replica states: " + e.getMessage());
            if (log.isDebugEnabled()) {
                e.printStackTrace();

            }
        }

        for (ReplicaResult result : requestedReplicas) {
            Identifier identifier = result.identifier;
            NodeReference nodeId = result.memberNode;
            SystemMetadata sysmeta = null;
            try {
                sysmeta = cn.getSystemMetadata(identifier);
            } catch (BaseException be) {
                log.error("Stale Replica Status: cannot get system metadata from CN for id: "
                        + identifier.getValue());
                continue;
            }

            MNode mn = null;
            try {
                mn = D1Client.getMN(nodeId);
            } catch (BaseException e) {
                log.error("Couldn't connect to the MN to manage replica states: " + e.getMessage());
                if (log.isDebugEnabled()) {
                    e.printStackTrace();
                }
                continue;
            }

            try {
                mn.getChecksum(identifier, sysmeta.getChecksum().getAlgorithm());
            } catch (BaseException e) {
                log.debug("Stale Replica Status Audit: Cannot get checksum from MN: "
                        + nodeId.getValue() + " for pid: " + identifier.getValue());
                continue;
            }

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
                continue;
            }
        }
    }

}
