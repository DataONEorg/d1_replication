package org.dataone.service.cn.replication.v1;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.core.ILock;

public class ReplicationService {

    private CNode cn;

    public static Log log = LogFactory.getLog(ReplicationService.class);

    public ReplicationService() {
        initializeCN();
    }

    public void requestReplica(Identifier identifier, NodeReference targetNode) {

        if (identifier == null || targetNode == null) {
            return;
        }
        if (cn == null) {
            log.error("Unable to request replicas - CN is null.");
            return;
        }

        ReplicationStatus status = null;

        // variables for hzProcess component coordination only
        ILock lock = null;
        String lockString = null;
        boolean isLocked = false;

        // a flag showing an already existing replica on the target MN
        boolean exists = false;

        // a flag for success on setting replication status
        boolean success = false;

        boolean updated = false;

        // a flag showing if the replica entry was deleted due to comm problems
        boolean deleted = false;

        // session is null - certificate is used
        Session session = null;

        MNode targetMN = getMemberNode(identifier, targetNode);

        SystemMetadata sysmeta = getSystemMetadata(identifier, session);

        NodeReference originatingNode = sysmeta.getAuthoritativeMemberNode();

        try {
            if (targetMN != null) {
                // check to be sure the replica is not requested or completed
                List<Replica> replicaList = sysmeta.getReplicaList();
                boolean handled = false;
                for (Replica replica : replicaList) {
                    NodeReference listedNode = replica.getReplicaMemberNode();
                    ReplicationStatus currentStatus = replica.getReplicationStatus();
                    if (listedNode == targetNode) {
                        if (currentStatus == ReplicationStatus.REQUESTED
                                || currentStatus == ReplicationStatus.COMPLETED) {
                            handled = true;
                            break;

                        }
                    } else {
                        continue;

                    }
                }
                // call for the replication
                if (!handled) {

                    // check if the object exists on the target MN already
                    try {
                        DescribeResponse description = targetMN.describe(identifier);
                        if (description.getDataONE_Checksum().equals(sysmeta.getChecksum())) {
                            exists = true;

                        }

                    } catch (NotFound nfe) {
                        // set the status to REQUESTED to avoid race conditions
                        // across CN threads handling replication tasks
                        status = ReplicationStatus.REQUESTED;

                        updated = this.cn
                                .setReplicationStatus(identifier, targetNode, status, null);
                        log.debug("Called setReplicationStatus() for identifier "
                                + identifier.getValue() + ". updated result: " + updated);

                        success = targetMN.replicate(session, sysmeta, originatingNode);
                        log.info("Called replicate() at targetNode " + targetNode.getValue()
                                + ", identifier " + identifier.getValue() + ". Success: " + success);
                    }

                } else {
                    log.info("Replica is already handled for " + targetNode.getValue()
                            + ", identifier " + identifier.getValue());

                }

            } else {
                log.error("Can't get system metadata: CNode object is null for identifier "
                        + identifier.getValue() + ", target node " + targetNode.getValue());
                success = false;
            }

        } catch (BaseException e) {
            log.error(
                    "Caught base exception attempting to call replicate for pid: "
                            + identifier.getValue() + " with exception: " + e.getDescription()
                            + " and message: " + e.getMessage(), e);
            try {
                log.info("The call to MN.replicate() failed for " + identifier.getValue() + " on "
                        + targetNode.getValue() + ". Trying again in 5 seconds.");
                Thread.sleep(5000L);
                try {
                    DescribeResponse description = targetMN.describe(identifier);
                    if (description.getDataONE_Checksum().equals(sysmeta.getChecksum())) {
                        exists = true;
                    }

                } catch (NotFound nf) {
                    sysmeta = cn.getSystemMetadata(session, identifier);
                    success = targetMN.replicate(session, sysmeta, originatingNode);
                    log.info("Called replicate() at targetNode " + targetNode.getValue()
                            + ", identifier " + identifier.getValue() + ". Success: " + success);
                }
            } catch (BaseException e1) {
                log.error("Caught base exception attempting to call replicate for pid: "
                        + identifier.getValue() + " with exception: " + e.getDescription()
                        + " and message: " + e.getMessage(), e);
                // still couldn't call replicate() successfully. fail.
                log.error(
                        "There was a second problem calling replicate() on "
                                + targetNode.getValue() + " for identifier "
                                + identifier.getValue(), e1);
                success = false;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while calling replicate() for identifier "
                        + identifier.getValue() + ", target node " + targetNode.getValue(), ie);
                success = false;

            }

        } catch (Exception e) {
            log.error(
                    "Unknown exception during replication for identifier " + identifier.getValue()
                            + ", target node " + targetNode.getValue() + ". Error message: "
                            + e.getMessage(), e);
            success = false;

        }

        // set the replication status
        if (success) {
            status = ReplicationStatus.REQUESTED;

        } else {
            status = ReplicationStatus.FAILED;

        }

        // for replicas that already exist, just update the system metadata
        if (exists) {
            status = ReplicationStatus.COMPLETED;

        }

        // if the status hasn't already been updated, update it
        if (!updated) {

            // depending on the status, update the status or delete the entry
            // in the system metadata for this identifier
            if (this.cn != null) {

                if (!status.equals(ReplicationStatus.FAILED)) {
                    // make every effort to update the status correctly
                    try {
                        updated = this.cn.setReplicationStatus(session, identifier, targetNode,
                                status, null);

                    } catch (BaseException be) {

                        // the replica has already completed from a different
                        // task
                        if (be instanceof InvalidRequest) {
                            log.warn(
                                    "Couldn't set the replication status to " + status.toString()
                                            + ", it may have possibly "
                                            + "already been set to completed for identifier "
                                            + identifier.getValue() + " and target node "
                                            + targetNode.getValue() + ". The error was: "
                                            + be.getMessage(), be);
                            return;

                        }

                        // there's trouble communicating with the local CN
                        log.error("There was a problem setting the replication status to "
                                + status.toString() + "  for identifier " + identifier.getValue());
                        // try the router CN address
                        updated = setReplicationStatus(session, identifier, targetNode, status,
                                null);
                    }

                } else {

                    // this task has failed. make every effort to delete the
                    // replica entry so the node prioritization is not skewed
                    // (due to factors not associated with the node)
                    try {
                        // call the local cn
                        deleted = this.cn.deleteReplicationMetadata(session, identifier,
                                targetNode, sysmeta.getSerialVersion().longValue());

                    } catch (BaseException be) {
                        // err. get the latest system metadata and call the cn
                        if (be instanceof VersionMismatch) {
                            try {
                                sysmeta = this.cn.getSystemMetadata(identifier);
                                deleted = this.cn.deleteReplicationMetadata(session, identifier,
                                        targetNode, sysmeta.getSerialVersion().longValue());

                            } catch (BaseException e) {
                                // we're really having difficulties. try the
                                // round robin CN address
                                deleted = deleteReplicationMetadata(session, identifier, targetNode);
                            }
                        }
                    }
                    // if we got to this state, something is very wrong with the
                    // CN environment. move on
                    if (!deleted) {
                        log.error("FAILED deletion of replica entry for identifier "
                                + identifier.getValue() + " and target node id "
                                + targetNode.getValue());
                    }
                }

            } else {
                if (!status.equals(ReplicationStatus.FAILED)) {
                    log.error("Can't update replica status for identifier " + identifier.getValue()
                            + " on node " + targetNode.getValue() + " to " + status.toString()
                            + ". CNode reference is null, trying the router address.");
                    // try setting the status against the router address
                    updated = setReplicationStatus(session, identifier, targetNode, status, null);

                } else {
                    log.error("Can't delete the replica entry for identifier "
                            + identifier.getValue() + " and node " + targetNode.getValue()
                            + ". CNode reference is null, trying the router address.");
                    // try deleting the entry against the router address
                    deleted = deleteReplicationMetadata(session, identifier, targetNode);

                }
            }
        }
        log.trace("METRICS:\tREPLICATION:\tEND QUEUE:\tidentifier:\t" + identifier.getValue()
                + "\tNODE:\t" + targetNode.getValue() + "\tSIZE:\t" + sysmeta.getSize().intValue());

        if (updated) {
            log.info("Updated replica status for identifier " + identifier.getValue() + " on node "
                    + targetNode.getValue() + " to " + status.toString());
            log.trace("METRICS:\tREPLICATION:\t" + status.toString().toUpperCase() + ":\tPID:\t"
                    + identifier.getValue() + "\tNODE:\t" + targetNode.getValue() + "\tSIZE:\t"
                    + sysmeta.getSize().intValue());

        } else {
            log.info("Didn't update replica status for identifier " + identifier.getValue()
                    + " on node " + targetNode.getValue() + " to " + status.toString());
        }

        if (deleted) {
            log.info("Deleted replica entry for identifier " + identifier.getValue() + " and node "
                    + targetNode.getValue());

        }
    }

    private SystemMetadata getSystemMetadata(Identifier identifier, Session session) {
        SystemMetadata sysmeta = null;
        try {
            sysmeta = cn.getSystemMetadata(session, identifier);
        } catch (BaseException be) {
            log.error("Cannot get system metedata for id: " + identifier.getValue());
        }
        return sysmeta;
    }

    /*
     * Set the replication status against the router CN address instead of the
     * local CN via D1Client. This may help with local CN communication trouble.
     */
    private boolean setReplicationStatus(Session session, Identifier pid, NodeReference targetNode,
            ReplicationStatus status, BaseException failure) {

        if (this.cn == null) {
            log.error("cannot set replication status, no CN object");
            return false;
        }
        boolean updated = false;

        // try multiple times since at this point we may be dealing with a lame
        // CN in the cluster and the RR may still point us to it
        for (int i = 0; i < 5; i++) {

            try {
                updated = cn.setReplicationStatus(session, pid, targetNode, status, null);
                if (updated) {
                    break;
                }
            } catch (BaseException be) {
                // the replica has already completed from a different task
                if (be instanceof InvalidRequest) {
                    log.warn("Couldn't set the replication status to " + status.toString()
                            + ", it may have possibly "
                            + "already been set to completed for identifier " + pid.getValue()
                            + " and target node " + targetNode.getValue() + ". The error was: "
                            + be.getMessage(), be);
                    return false;

                }
                if (log.isDebugEnabled()) {
                    log.debug(be);

                }
                log.error(
                        "Error in calling setReplicationStatus() for identifier " + pid.getValue()
                                + ", target node " + targetNode.getValue() + " and status of "
                                + status.toString() + ": " + be.getMessage(), be);
                continue;
            }

        }
        return updated;
    }

    /*
     * Delete the replica entry for the target node using the CN router URL
     * rather than the local CN via D1Client. This may help with local CN
     * communication trouble.
     * 
     * @param session - the CN session instance
     * 
     * @param - pid - the identifier of the object system metadata being
     * modified
     * 
     * @param targetNode - the node id of the replica target being deleted
     * 
     * @param serialVersion - the serialVersion of the system metadata being
     * operated on
     */
    private boolean deleteReplicationMetadata(Session session, Identifier pid,
            NodeReference targetNode) {

        if (this.cn == null) {
            log.error("cannot set replication status, no CN object");
            return false;
        }
        SystemMetadata sysmeta;
        boolean deleted = false;

        // try multiple times since at this point we may be dealing with a lame
        // CN in the cluster and the RR may still point us to it
        for (int i = 0; i < 5; i++) {
            try {
                // refresh the system metadata in case it changed
                sysmeta = cn.getSystemMetadata(pid);
                deleted = cn.deleteReplicationMetadata(pid, targetNode, sysmeta.getSerialVersion()
                        .longValue());
                if (deleted) {
                    break;
                }

            } catch (BaseException be) {
                if (log.isDebugEnabled()) {
                    log.debug(be);
                }
                log.error(
                        "Error in calling deleteReplicationMetadata() for identifier "
                                + pid.getValue() + " and target node " + targetNode.getValue()
                                + ": " + be.getMessage(), be);
                continue;

            } catch (RuntimeException re) {
                if (log.isDebugEnabled()) {
                    log.debug(re);
                }
                log.error("Error in getting sysyem metadata from the map for " + "identifier "
                        + pid.getValue() + ": " + re.getMessage(), re);
                continue;

            }
        }
        return deleted;
    }

    private void initializeCN() {
        try {
            this.cn = D1Client.getCN();
        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the CN ", e);
            // try again, then fail
            try {
                Thread.sleep(5000L);
                this.cn = D1Client.getCN();

            } catch (ServiceFailure e1) {
                log.warn("Second ServiceFailure while getting a reference to the CN", e1);
                this.cn = null;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the CN ", ie);
                this.cn = null;
            }
        }
    }

    private MNode getMemberNode(Identifier identifier, NodeReference targetNode) {
        MNode targetMN = null;
        // Get an target MNode reference to communicate with
        try {
            targetMN = D1Client.getMN(targetNode);

        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the MN ", e);
            try {
                Thread.sleep(5000L);
                targetMN = D1Client.getMN(targetNode);
            } catch (ServiceFailure e1) {
                log.error(
                        "There was a problem calling replicate() on for identifier "
                                + identifier.getValue(), e1);
                targetMN = null;
            } catch (InterruptedException ie) {
                log.error(
                        "Caught InterruptedException while getting a reference to the MN identifier "
                                + identifier.getValue() + ", target node " + targetNode.getValue(),
                        ie);
                targetMN = null;
            }
        }
        return targetMN;
    }

}
