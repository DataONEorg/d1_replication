package org.dataone.cn.data.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReplicationTaskRepository extends
        PagingAndSortingRepository<ReplicationTask, Long> {

    List<ReplicationTask> findByPid(String pid);

    List<ReplicationTask> findByStatusAndNextExecutionLessThan(String status, long time);

    List<ReplicationTask> findByStatusAndNextExecutionLessThan(String status, long time,
            Pageable page);

}
