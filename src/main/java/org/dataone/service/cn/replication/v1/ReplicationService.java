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

import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * 
 * Encapsulation of common operations involving replica system metadata objects.
 * 
 * Implementations make use of the CN REST api to perform updates/creates on
 * system metadata replica objects.
 * 
 * @author sroseboo
 * 
 */
public class ReplicationService {

    private CNode cn;

    public static Logger log = Logger.getLogger(ReplicationService.class);

    public ReplicationService() {
        initializeCN();
    }

    /**
     * Handles moving a 'queued' replication object into 'requested' state.
     * Replica is able to move to 'requested' if the replica request is
     * successfully made to the targetNode. If the target node cannot be
     * acquired, the replica object is removed. If the replica request fails at
     * the target member node, the replica is updated to 'failed' status.
     * 
     * @param identifier
     * @param targetNode
     */
    public void requestQueuedReplication(Identifier identifier, NodeReference targetNode) {

        if (identifier == null || targetNode == null) {
            return;
        }

        if (cn == null) {
            log.error("Unable to request replicas - CN is null.");
            return;
        }

        SystemMetadata sysmeta = null;
        try {
            sysmeta = getSystemMetadata(identifier);
        } catch (NotFound e) {
        }
        if (sysmeta == null) {
            log.error("Unable to get system metadata for: " + identifier + ". exiting...");
            return;
        }

        if (alreadyReplicated(sysmeta, targetNode)) {
            log.debug("Replica is already handled for " + targetNode.getValue() + ", identifier "
                    + identifier.getValue() + ". exiting...");
            return;
        }

        hasQueuedReplica(sysmeta, targetNode);
        // if (hasQueuedReplica(sysmeta, targetNode) == false) {
        // log.debug("Replica not queued for: " + identifier + " for node: "
        // + targetNode.getValue() + ". exiting...");
        // return;
        // }

        boolean updated = setReplicaToRequested(identifier, targetNode);
        if (updated == false) {
            log.error("Unable to set replication status to 'requested' for: " + identifier
                    + " for node: " + targetNode.getValue() + ". exiting...");
            return;
        }

        MNode targetMN = getMemberNode(targetNode);
        if (targetMN == null) {
            log.error("Unable to get target mn: " + targetNode.getValue()
                    + ". deleting replica metadata, not requesting replica for pid: "
                    + sysmeta.getIdentifier().getValue());
            deleteReplicationMetadata(identifier, targetNode);
            return;
        }

        boolean success = requestReplication(targetMN, sysmeta);

        if (!success) {
            log.error("Unable to request replica from target mn: " + targetNode.getValue()
                    + " for: " + identifier.getValue() + ". setting status to failed.");
            setReplicationStatus(identifier, targetNode, ReplicationStatus.FAILED);
        }
    }

    /**
     * Delete the replica entry for the target node using the CN router URL
     * rather than the local CN via D1Client. This may help with local CN
     * communication trouble.
     * 
     * @param pid
     *            - the identifier of the object system metadata being modified
     * @param targetNode
     *            - the node id of the replica target being deleted
     * @param serialVersion
     *            - the serialVersion of the system metadata being operated on
     **/
    public boolean deleteReplicationMetadata(Identifier pid, NodeReference targetNode) {

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
                sysmeta = getSystemMetadata(pid);
                deleted = cn.deleteReplicationMetadata(pid, targetNode, sysmeta.getSerialVersion()
                        .longValue());
                if (deleted) {
                    break;
                }
            } catch (BaseException be) {
                log.error(
                        "BaseException error in calling deleteReplicationMetadata() for identifier "
                                + pid.getValue() + " and target node " + targetNode.getValue()
                                + ": " + be.getMessage(), be);
            } catch (RuntimeException re) {
                log.error(
                        "Runtime exception calling delete replica metadata for: " + pid.getValue()
                                + " for node: " + targetNode.getValue(), re);
            }
        }
        if (!deleted) {
            log.error("Ultimately unable to delete replica metadata for: " + pid + " on node: "
                    + targetNode.getValue());
        }
        return deleted;
    }

    public boolean setReplicaToRequested(Identifier identifier, NodeReference targetNode) {
        return setReplicationStatus(identifier, targetNode, ReplicationStatus.REQUESTED);
    }

    public boolean setReplicaToCompleted(Identifier identifier, NodeReference targetNode) {
        return setReplicationStatus(identifier, targetNode, ReplicationStatus.COMPLETED);
    }

    /**
     * Set the replication status against the router CN address instead of the
     * local CN via D1Client. This may help with local CN communication trouble.
     **/
    private boolean setReplicationStatus(Identifier pid, NodeReference targetNode,
            ReplicationStatus status) {

        if (this.cn == null) {
            log.error("cannot set replication status, no CN object");
            return false;
        }
        boolean updated = false;
        for (int i = 0; i < 5; i++) {
            try {
                updated = cn.setReplicationStatus(pid, targetNode, status, null);
                if (updated) {
                    break;
                }
            } catch (InvalidRequest ire) {
                log.warn(
                        "Couldn't set the replication status to " + status.toString()
                                + ", it may have possibly "
                                + "already been set to completed for identifier " + pid.getValue()
                                + " and target node " + targetNode.getValue() + ". The error was: "
                                + ire.getMessage(), ire);
            } catch (BaseException be) {
                log.error(
                        "Error in calling setReplicationStatus() for identifier " + pid.getValue()
                                + ", target node " + targetNode.getValue() + " and status of "
                                + status.toString() + ": " + be.getMessage(), be);
            }
        }
        if (!updated) {
            log.error("Ultimately unable to update status: " + status + " for: " + pid
                    + " on node: " + targetNode.getValue());
        }
        return updated;
    }

    private boolean alreadyReplicated(SystemMetadata sysmeta, NodeReference targetNode) {
        boolean handled = false;
        List<Replica> replicaList = sysmeta.getReplicaList();
        for (Replica replica : replicaList) {
            NodeReference listedNode = replica.getReplicaMemberNode();
            if (listedNode.getValue().equals(targetNode.getValue())) {
                ReplicationStatus currentStatus = replica.getReplicationStatus();
                if (currentStatus == ReplicationStatus.REQUESTED
                        || currentStatus == ReplicationStatus.COMPLETED) {
                    handled = true;
                    break;
                }
            }
        }
        if (handled) {
            log.debug("Replica is already handled for: " + sysmeta.getIdentifier().getValue()
                    + " at node: " + targetNode.getValue());
        }
        return handled;
    }

    private boolean hasQueuedReplica(SystemMetadata sysmeta, NodeReference targetNode) {
        boolean queued = false;
        List<Replica> replicaList = sysmeta.getReplicaList();
        for (Replica replica : replicaList) {
            NodeReference listedNode = replica.getReplicaMemberNode();
            if (listedNode.getValue().equals(targetNode.getValue())) {
                ReplicationStatus currentStatus = replica.getReplicationStatus();
                if (currentStatus == ReplicationStatus.QUEUED) {
                    queued = true;
                    break;
                }
            }
        }
        if (!queued) {
            log.debug("Replica is not queued for: " + sysmeta.getIdentifier().getValue()
                    + " at node: " + targetNode.getValue());
        }
        return queued;
    }

    public SystemMetadata getSystemMetadata(Identifier identifier) throws NotFound {
        SystemMetadata sysmeta = null;
        if (identifier != null && identifier.getValue() != null) {
            try {
                sysmeta = cn.getSystemMetadata(identifier);
            } catch (InvalidToken e) {
                log.error("Cannot get system metedata for id: " + identifier.getValue(), e);
            } catch (ServiceFailure e) {
                log.error("Cannot get system metedata for id: " + identifier.getValue(), e);
            } catch (NotAuthorized e) {
                log.error("Cannot get system metedata for id: " + identifier.getValue(), e);
            } catch (NotImplemented e) {
                log.error("Cannot get system metedata for id: " + identifier.getValue(), e);
            }
        }
        return sysmeta;
    }

    public InputStream getObjectFromCN(Identifier identifier) throws NotFound {
        InputStream is = null;
        if (identifier != null && identifier.getValue() != null) {
            try {
                is = cn.get(identifier);
            } catch (InvalidToken e) {
                log.error("Unable to get object from CN for pid: " + identifier.getValue(), e);
            } catch (ServiceFailure e) {
                log.error("Unable to get object from CN for pid: " + identifier.getValue(), e);
            } catch (NotAuthorized e) {
                log.error("Unable to get object from CN for pid: " + identifier.getValue(), e);
            } catch (NotImplemented e) {
                log.error("Unable to get object from CN for pid: " + identifier.getValue(), e);
            }
        }
        return is;
    }

    private boolean requestReplication(MNode targetMN, SystemMetadata sysmeta) {

        if (sysmeta == null) {
            return false;
        }

        NodeReference originatingNode = determineReplicationSourceNode(sysmeta);
        if (originatingNode == null) {
            log.error("Could not determine replication source node for replication request for pid: "
                    + sysmeta.getIdentifier().getValue() + ".  Replication request failed.");
            return false;
        }
        boolean success = false;
        try {
            success = targetMN.replicate(sysmeta, originatingNode);
            log.info("Called replicate() at targetNode " + targetMN.getNodeId() + ", identifier "
                    + sysmeta.getIdentifier().getValue() + ". Success: " + success);
        } catch (BaseException e) {
            log.error("Caught base exception attempting to call replicate for pid: "
                    + sysmeta.getIdentifier().getValue() + " with exception: " + e.getDescription()
                    + " and message: " + e.getMessage(), e);
            try {
                log.info("The call to MN.replicate() failed for "
                        + sysmeta.getIdentifier().getValue() + " on " + targetMN.getNodeId()
                        + ". Trying again in 5 seconds.");
                Thread.sleep(5000L);

                sysmeta = getSystemMetadata(sysmeta.getIdentifier());
                if (sysmeta != null) {
                    success = targetMN.replicate(sysmeta, originatingNode);
                    log.info("Called replicate() at targetNode " + targetMN.getNodeId()
                            + ", identifier " + sysmeta.getIdentifier().getValue() + ". Success: "
                            + success);
                }
            } catch (BaseException e1) {
                log.error(
                        "Caught base exception attempting to call replicate for pid: "
                                + sysmeta.getIdentifier().getValue() + " with exception: "
                                + e.getDescription() + " and message: " + e.getMessage(), e);
                log.error(
                        "There was a second problem calling replicate() on " + targetMN.getNodeId()
                                + " for identifier " + sysmeta.getIdentifier().getValue(), e1);
            } catch (InterruptedException ie) {
                log.error(
                        "Caught InterruptedException while calling replicate() for identifier "
                                + sysmeta.getIdentifier().getValue() + ", target node "
                                + targetMN.getNodeId(), ie);
            }
        } catch (Exception e) {
            log.error("Unknown exception during replication for identifier "
                    + sysmeta.getIdentifier().getValue() + ", target node " + targetMN.getNodeId()
                    + ". Error message: " + e.getMessage(), e);
        }
        return success;
    }

    private NodeReference determineReplicationSourceNode(SystemMetadata sysMeta) {
        NodeReference source = null;
        NodeReference authNode = sysMeta.getAuthoritativeMemberNode();
        for (Replica replica : sysMeta.getReplicaList()) {
            if (replica.getReplicaMemberNode().equals(authNode)
                    && replica.getReplicationStatus().equals(ReplicationStatus.COMPLETED)) {
                source = authNode;
                break;
            } else if (source == null
                    && replica.getReplicationStatus().equals(ReplicationStatus.COMPLETED)) {
                // set the source to the first completed replica but keep iterating to find
                // the authoritative MN and give preference to its 'replica' as source.
                source = replica.getReplicaMemberNode();
            }
        }
        return source;
    }

    /**
     * Update the replica metadata against the CN router address rather than the
     * local CN address. This only gets called if normal updates fail due to
     * local CN communication errors
     * 
     * @return true if the replica metadata are updated
     * @param session
     * @param pid
     * @param replicaMetadata
     **/
    public boolean updateReplicationMetadata(Identifier pid, Replica replicaMetadata) {

        SystemMetadata sysmeta = null;
        boolean updated = false;

        for (int i = 0; i < 5; i++) {
            try {
                // refresh the system metadata in case it changed
                sysmeta = getSystemMetadata(pid);
                updated = cn.updateReplicationMetadata(pid, replicaMetadata, sysmeta
                        .getSerialVersion().longValue());
                if (updated) {
                    break;
                }
            } catch (BaseException be) {
                // the replica has already completed from a different task
                if (be instanceof InvalidRequest) {
                    log.warn(
                            "Couldn't update replication metadata to "
                                    + replicaMetadata.getReplicationStatus().toString()
                                    + ", it may have possibly already been updated for identifier "
                                    + pid.getValue() + " and target node "
                                    + replicaMetadata.getReplicaMemberNode().getValue()
                                    + ". The error was: " + be.getMessage(), be);
                    return false;
                }
                if (log.isDebugEnabled()) {
                    log.debug(be);
                }
                log.error("Error in calling updateReplicationMetadata(): " + be.getMessage());
                continue;
            } catch (RuntimeException re) {
                if (log.isDebugEnabled()) {
                    log.debug(re);
                }
                log.error("Error in getting sysyem metadata from the map: " + re.getMessage());
                continue;
            }
        }
        return updated;
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

    public MNode getMemberNode(NodeReference targetNode) {
        MNode targetMN = null;
        try {
            targetMN = D1Client.getMN(targetNode);
        } catch (ServiceFailure e) {
            log.warn(
                    "Caught a ServiceFailure while getting a reference to the MN: "
                            + targetNode.getValue(), e);
            try {
                Thread.sleep(5000L);
                targetMN = D1Client.getMN(targetNode);
            } catch (ServiceFailure e1) {
                log.error(
                        "Second service failure getting reference to MN: " + targetNode.getValue(),
                        e1);
            } catch (InterruptedException ie) {
                log.error(
                        "Caught InterruptedException while getting a reference to the MN identifier, target node "
                                + targetNode.getValue(), ie);
            }
        }
        if (targetMN != null) {
            targetMN.setNodeId(targetNode.getValue());
        }
        return targetMN;
    }
}
