package org.dataone.service.cn.replication.auditor.v1.strategy;

import java.util.Date;

import org.apache.log4j.Logger;
import org.dataone.cn.log.AuditEvent;
import org.dataone.cn.log.AuditLogClientFactory;
import org.dataone.cn.log.AuditLogEntry;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.cn.replication.ReplicationCommunication;
import org.dataone.service.cn.replication.ReplicationFactory;
import org.dataone.service.cn.replication.ReplicationService;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v2.SystemMetadata;

/**
 * Replication Auditing Delegate class - encapulsates common replication auditing logic
 * used across the different replica auditing strategies.
 * 
 * @author sroseboo
 */
public class ReplicaAuditingDelegate {

    public static Logger log = Logger.getLogger(ReplicaAuditingDelegate.class);

    private static final String cnRouterId = Settings.getConfiguration().getString(
            "cn.router.nodeId", "urn:node:CN");

    private ReplicationService replicationService;
    private NodeRegistryService nodeService = new NodeRegistryService();

    public ReplicaAuditingDelegate() {
        replicationService = ReplicationFactory.getReplicationService();
    }

    /**
     * Retrieves system meta data from CN cluster and logs appropriate
     * audit log message if not able to retrieve.  First removes any
     * existing log message that would be generated by previous error
     * in this method.
     * 
     * @param pid
     * @return
     */
    protected SystemMetadata getSystemMetadata(Identifier pid) {

        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                new AuditLogEntry(pid.getValue(), cnRouterId, AuditEvent.REPLICA_AUDIT_FAILED,
                        null, null));
        SystemMetadata sysMeta = null;
        try {
            sysMeta = replicationService.getSystemMetadata(pid);
        } catch (NotFound e) {
            log.error("Could not find system meta for pid: " + pid.getValue());
        }
        if (sysMeta == null) {
            log.error("Cannot get system metadata from CN for pid: " + pid.getValue()
                    + ".  Could not replicas for pid: " + pid.getValue() + "");
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), cnRouterId,
                    AuditEvent.REPLICA_AUDIT_FAILED,
                    "Unable to audit replica.  Could not retrieve system metadata for pid: "
                            + pid.getValue() + " from the CN cluster for replication auditing.");
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
        }
        return sysMeta;
    }

    protected void updateVerifiedReplica(Identifier pid, Replica replica) {
        replica.setReplicationStatus(ReplicationStatus.COMPLETED);
        replica.setReplicaVerified(this.calculateReplicaVerifiedDate());
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica verified date  for pid: " + pid + " on CN");
        }
        AuditLogClientFactory.getAuditLogClient().removeReplicaAuditEvent(
                new AuditLogEntry(pid.getValue(), replica.getReplicaMemberNode().getValue(), null,
                        null, null));
    }

    protected void updateInvalidReplica(SystemMetadata sysMeta, Replica replica) {

        boolean isAuthMNReplica = this.isAuthoritativeMNReplica(sysMeta, replica);
        Identifier pid = sysMeta.getIdentifier();

        if (isAuthMNReplica == true) {
            AuditLogEntry logEntry = new AuditLogEntry(pid.getValue(), replica
                    .getReplicaMemberNode().getValue(), AuditEvent.REPLICA_AUDIT_FAILED,
                    "For pid: " + pid.getValue()
                            + "  Authoritative Member Node replica is not valid."
                            + "  Not marked INVALID.  Replica verified date updated.");
            AuditLogClientFactory.getAuditLogClient().logAuditEvent(logEntry);
        } else {
            replica.setReplicationStatus(ReplicationStatus.INVALIDATED);
        }
        replica.setReplicaVerified(this.calculateReplicaVerifiedDate());
        boolean success = replicationService.updateReplicationMetadata(pid, replica);
        if (!success) {
            log.error("Cannot update replica status to INVALID for pid: " + pid + " on MN: "
                    + replica.getReplicaMemberNode().getValue());
        }
    }

    protected Checksum getChecksumFromMN(Identifier pid, SystemMetadata sysMeta,
            NodeReference nodeRef) throws BaseException {

        ReplicationCommunication rc = ReplicationCommunication.getInstance(nodeRef);

        Checksum checksum = null;
        for (int i = 0; i < 5; i++) {
            try {
                checksum = rc.getChecksumFromMN(pid, nodeRef, sysMeta);
                break;
            } catch (BaseException e) {
                if (i >= 4) {
                    throw e;
                }
            }
        }
        return checksum;
    }

    protected boolean isCNodeReplica(Replica replica) {
        boolean isCNodeReplica = false;
        if (replica != null && replica.getReplicaMemberNode() != null) {
            Node node = null;
            try {
                node = nodeService.getNode(replica.getReplicaMemberNode());
            } catch (ServiceFailure e) {
                log.error("Unable to get node from node registry service for node ref: "
                        + replica.getReplicaMemberNode().getValue(), e);
                e.printStackTrace();
            } catch (NotFound e) {
                log.error("Unable to get node from node registry service for node ref: "
                        + replica.getReplicaMemberNode().getValue(), e);
                e.printStackTrace();
            }
            if (node != null && node.getType() != null) {
                isCNodeReplica = NodeType.CN.equals(node.getType());
            }
        }
        return isCNodeReplica;
    }

    protected Date calculateReplicaVerifiedDate() {
        return new Date(System.currentTimeMillis());
    }

    protected boolean isAuthoritativeMNReplica(SystemMetadata sysMeta, Replica replica) {
        return replica.getReplicaMemberNode().getValue()
                .equals(sysMeta.getAuthoritativeMemberNode().getValue());
    }

    protected String getCnRouterId() {
        return cnRouterId;
    }
}
