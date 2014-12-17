package org.dataone.service.cn.replication;

import org.dataone.cn.data.repository.ReplicationAttemptHistoryRepository;
import org.dataone.cn.data.repository.ReplicationTaskRepository;

public interface ReplicationRepositoryFactory {

    public ReplicationAttemptHistoryRepository getReplicationTryHistoryRepository();

    public ReplicationTaskRepository getReplicationTaskRepository();

    public void initContext();

    public void closeContext();

}
