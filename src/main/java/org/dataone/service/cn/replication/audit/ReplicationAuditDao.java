package org.dataone.service.cn.replication.audit;

import java.util.Date;

import org.dataone.service.types.v1.Replica;
import org.springframework.beans.support.PagedListHolder;

/**
 * Abstract definition of the replication auditing data access object used to
 * identify replicas that need to be audited.
 * 
 * Also defines interface for updating the replication auditing data store.
 * 
 * @author sroseboo
 * 
 */
public interface ReplicationAuditDao {

    public PagedListHolder<ReplicaAuditDto> getReplicasByDate(Date auditDate, int pageSize,
            int pageNumber);

    public PagedListHolder<ReplicaAuditDto> getFailedReplicas(int pageSize, int pageNumber);

    public PagedListHolder<ReplicaAuditDto> getInvalidReplicas(int pageSize, int pageNumber);

    public PagedListHolder<ReplicaAuditDto> getStaleQueuedRelicas(int pageSize, int pageNumber);

    public void updateReplica(Replica replica);
}
