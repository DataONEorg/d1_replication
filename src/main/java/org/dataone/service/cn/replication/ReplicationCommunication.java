package org.dataone.service.cn.replication;

import org.apache.log4j.Logger;
import org.dataone.service.cn.replication.v2.MNCommunication;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.SystemMetadata;

public abstract class ReplicationCommunication {

    public static Logger log = Logger.getLogger(ReplicationCommunication.class);

    protected ReplicationService replicationService = ReplicationFactory.getReplicationService();

    public abstract boolean requestReplication(NodeReference targetNodeId, SystemMetadata sysmeta)
            throws BaseException;

    public abstract Checksum getChecksumFromMN(Identifier identifier, NodeReference nodeId,
            SystemMetadata sysmeta) throws NotFound, BaseException;

    public static ReplicationCommunication getInstance(NodeReference targetNodeId) {

        Node targetNode = ReplicationFactory.getReplicationManager().getNode(targetNodeId);

        // default to v1, but use v2 if we find it enabled
        ReplicationCommunication impl = new org.dataone.service.cn.replication.v1.MNCommunication();

        if (targetNode != null && targetNode.getServices() != null
                && targetNode.getServices().getServiceList() != null) {
            for (Service service : targetNode.getServices().getServiceList()) {
                if (service.getName().equals("MNReplication") && service.getVersion().equals("v2")) {
                    impl = new MNCommunication();
                    break;
                }
            }
        }

        return impl;
    }
}
