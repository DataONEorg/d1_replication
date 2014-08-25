package org.dataone.cn.data.repository;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReplicationAttemptHistoryRepository extends
        PagingAndSortingRepository<ReplicationAttemptHistory, Long> {

    List<ReplicationAttemptHistory> findByPid(String pidValue);

    List<ReplicationAttemptHistory> findByNodeId(String nodeId);

    // TODO: write test for this
    List<ReplicationAttemptHistory> findByPidAndNodeId(String pidValue, String nodeId);

    List<ReplicationAttemptHistory> findByReplicationAttempts(Integer attempts);

    Page<ReplicationAttemptHistory> findByPid(String pidValue, Pageable page);
}
