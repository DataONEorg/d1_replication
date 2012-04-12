package org.dataone.service.cn.replication.audit;

import java.util.Date;

import org.dataone.service.types.v1.Identifier;
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

    public PagedListHolder<Identifier> getReplicasByDate(Date auditDate, int pageSize,
            int pageNumber);

    public PagedListHolder<Identifier> getFailedReplicas(int pageSize, int pageNumber);

    public PagedListHolder<Identifier> getInvalidReplicas(int pageSize, int pageNumber);

    public PagedListHolder<Identifier> getStaleQueuedRelicas(int pageSize, int pageNumber);

    public void updateReplica(Replica replica);
}
