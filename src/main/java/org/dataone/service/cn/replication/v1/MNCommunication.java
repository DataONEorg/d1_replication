package org.dataone.service.cn.replication.v1;

import java.lang.reflect.InvocationTargetException;

import org.dataone.client.v1.MNode;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.service.cn.replication.ReplicationCommunication;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.types.v2.TypeFactory;

public class MNCommunication extends ReplicationCommunication {

    @Override
    public boolean requestReplication(NodeReference targetNodeId, SystemMetadata sysmeta)
            throws BaseException {

        if (sysmeta == null) {
            return false;
        }
        org.dataone.service.types.v1.SystemMetadata v1SysMeta = getV1SystemMetadata(sysmeta);
        if (v1SysMeta == null) {
            return false;
        }

        // get MNode reference for the correct version
        MNode targetMN = D1Client.getMN(targetNodeId);
        if (targetMN == null) {
            log.error("Unable to get target mn: " + targetNodeId.getValue()
                    + ". deleting replica metadata, not requesting replica for pid: "
                    + v1SysMeta.getIdentifier().getValue());
            return false;
        }

        NodeReference originatingNode = replicationService.determineReplicationSourceNode(sysmeta);
        if (originatingNode == null) {
            log.error("Could not determine replication source node for replication request for pid: "
                    + v1SysMeta.getIdentifier().getValue() + ".  Replication request failed.");
            return false;
        }
        boolean success = false;
        try {
            success = targetMN.replicate(v1SysMeta, originatingNode);
            log.info("Called replicate() at targetNode " + targetNodeId.getValue()
                    + ", identifier " + v1SysMeta.getIdentifier().getValue() + ". Success: "
                    + success);
        } catch (BaseException e) {
            log.error(
                    "Caught base exception attempting to call replicate for pid: "
                            + v1SysMeta.getIdentifier().getValue() + " at target: "
                            + targetNodeId.getValue() + " with exception: " + e.getDescription()
                            + " and message: " + e.getMessage(), e);
            try {
                log.info("The call to MN.replicate() failed for "
                        + v1SysMeta.getIdentifier().getValue() + " on " + targetNodeId.getValue()
                        + ". Trying again in 5 seconds.");
                Thread.sleep(5000L);

                sysmeta = replicationService.getSystemMetadata(sysmeta.getIdentifier());
                v1SysMeta = getV1SystemMetadata(sysmeta);
                if (v1SysMeta != null) {
                    success = targetMN.replicate(v1SysMeta, originatingNode);
                    log.info("Called replicate() at targetNode " + targetNodeId.getValue()
                            + ", identifier " + v1SysMeta.getIdentifier().getValue()
                            + ". Success: " + success);
                }
            } catch (BaseException e1) {
                log.error(
                        "Caught base exception attempting to call replicate for pid: "
                                + v1SysMeta.getIdentifier().getValue() + " with exception: "
                                + e.getDescription() + " and message: " + e.getMessage(), e);
                log.error(
                        "There was a second problem calling replicate() on "
                                + targetNodeId.getValue() + " for identifier "
                                + v1SysMeta.getIdentifier().getValue(), e1);
            } catch (InterruptedException ie) {
                log.error(
                        "Caught InterruptedException while calling replicate() for identifier "
                                + v1SysMeta.getIdentifier().getValue() + ", target node "
                                + targetNodeId.getValue(), ie);
            }
        } catch (Exception e) {
            log.error(
                    "Unknown exception during replication for identifier "
                            + v1SysMeta.getIdentifier().getValue() + ", target node "
                            + targetNodeId.getValue() + ". Error message: " + e.getMessage(), e);
        }
        return success;
    }

    @Override
    public Checksum getChecksumFromMN(Identifier identifier, NodeReference nodeId,
            SystemMetadata sysmeta) throws NotFound, BaseException {

        MNode mn = D1Client.getMN(nodeId);

        Checksum mnChecksum = null;
        try {
            mnChecksum = mn.getChecksum(identifier, sysmeta.getChecksum().getAlgorithm());
        } catch (NotFound e) {
            log.debug(
                    "Checksum NotFound from MN: " + nodeId.getValue() + " for pid: "
                            + identifier.getValue(), e);
        } catch (BaseException e) {
            log.debug("Cannot get checksum from MN: " + nodeId.getValue() + " for pid: "
                    + identifier.getValue(), e);
        }
        return mnChecksum;
    }

    private org.dataone.service.types.v1.SystemMetadata getV1SystemMetadata(SystemMetadata sysmeta) {
        org.dataone.service.types.v1.SystemMetadata v1SysMeta = null;
        try {
            v1SysMeta = TypeFactory.convertTypeFromType(sysmeta,
                    org.dataone.service.types.v1.SystemMetadata.class);
        } catch (InstantiationException e2) {
            log.error(
                    "Cannot request replication - unable to convert v2 System metadata to v1 for pid: "
                            + sysmeta.getIdentifier().getValue(), e2);
        } catch (IllegalAccessException e2) {
            log.error(
                    "Cannot request replication - unable to convert v2 System metadata to v1 for pid: "
                            + sysmeta.getIdentifier().getValue(), e2);
        } catch (InvocationTargetException e2) {
            log.error(
                    "Cannot request replication - unable to convert v2 System metadata to v1 for pid: "
                            + sysmeta.getIdentifier().getValue(), e2);
        } catch (NoSuchMethodException e2) {
            log.error(
                    "Cannot request replication - unable to convert v2 System metadata to v1 for pid: "
                            + sysmeta.getIdentifier().getValue(), e2);
        }
        return v1SysMeta;
    }

}
