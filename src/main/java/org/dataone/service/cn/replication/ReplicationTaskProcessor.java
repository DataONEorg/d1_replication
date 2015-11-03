package org.dataone.service.cn.replication;

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.configuration.Settings;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

public class ReplicationTaskProcessor implements Runnable {

    private static Logger log = Logger.getLogger(ReplicationTaskProcessor.class);

    private static ReplicationTaskRepository taskRepository = ReplicationFactory
            .getReplicationTaskRepository();
    private static ReplicationManager replicationManager = ReplicationFactory
            .getReplicationManager();

    private static final int PAGE_SIZE = Settings.getConfiguration().getInt(
            "dataone.cn.replication.task.page.size", 200);

    @Override
    public void run() {
        if (ComponentActivationUtility.replicationIsActive()) {
            log.debug("Replication task processor executing.");
            long now = System.currentTimeMillis();
            Pageable page = new PageRequest(0, PAGE_SIZE);
            List<ReplicationTask> taskList = taskRepository
                    .findByStatusAndNextExecutionLessThanOrderByNextExecutionAsc(
                            ReplicationTask.STATUS_NEW, now, page);
            log.debug("Replication task processor found: " + taskList.size() + " tasks to process.");
            for (ReplicationTask task : taskList) {
                task.markInProcess();
                taskRepository.save(task);
                replicationManager
                        .createAndQueueTasks(D1TypeBuilder.buildIdentifier(task.getPid()));
            }
        }
    }
}
