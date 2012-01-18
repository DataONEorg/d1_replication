/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.service.cn.replication.v1;

import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;

/**
 *
 * @author waltz
 */
public class CNReplicationImpl implements CNReplication {

    @Override
    public boolean setReplicationStatus(Session session, Identifier pid, NodeReference nodeRef, ReplicationStatus status, BaseException failure) throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, InvalidRequest, NotFound {
        return true;
    }

    @Override
    public boolean setReplicationPolicy(Session session, Identifier pid, ReplicationPolicy policy, long serialVersion) throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch {
        return true;
    }

    @Override
    public boolean isNodeAuthorized(Session originatingNodeSession, Subject targetNodeSubject, Identifier pid) throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure, NotFound, InvalidRequest {
        return true;
    }

    @Override
    public boolean updateReplicationMetadata(Session targetNodeSession, Identifier pid, Replica replicaMetadata, long serialVersion) throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, InvalidRequest, InvalidToken, VersionMismatch {
        return true;
    }

    @Override
    public boolean deleteReplicationMetadata(Session session, Identifier pid, NodeReference nodeId, long serialVersion) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, VersionMismatch {
       return true;
    }

}
