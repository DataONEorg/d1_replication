package org.dataone.service.cn.replication.audit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.springframework.beans.support.PagedListHolder;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IdGenerator;

public class ReplicaAuditTaskGenerator {

    private ReplicationAuditDao auditDao;
    private int pageSize = 0;
    private int taskChunkSize = 1;

    // how is the default instance configuration provided??????
    // client to the processing cluster
    private HazelcastClient processingClient;

    private IQueue<MNAuditTask> replicaAuditTaskQueue;

    private Set<Identifier> processingReplicaAuditIdentifiers;

    private Lock auditGenerationLock;

    // DOES THIS NEED TO BE DEFINED IN HZCONFIG??????
    private IdGenerator taskIdGenerator;

    private static final String HZ_AUDIT_LOCK_NAME = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.generation.lock");

    private static final String REPLICA_AUDIT_TASK_QUEUE_NAME = Settings.getConfiguration()
            .getString("dataone.hazelcast.replication.audit.task.queue");

    private static final String REPLICA_AUDIT_SET_HANDLED = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.processing.identifiers");

    private static final String REPLICA_AUDIT_IDS = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.task.ids");

    private static Logger log = Logger.getLogger(ReplicaAuditTaskGenerator.class.getName());

    public ReplicaAuditTaskGenerator() {
    }

    private void startHazelcastClient() {
        if (this.processingClient == null) {
            this.processingClient = Hazelcast.getDefaultInstance();
            replicaAuditTaskQueue = processingClient.getQueue(REPLICA_AUDIT_TASK_QUEUE_NAME);
            processingReplicaAuditIdentifiers = processingClient.getSet(REPLICA_AUDIT_SET_HANDLED);
            taskIdGenerator = processingClient.getIdGenerator(REPLICA_AUDIT_IDS);
            auditGenerationLock = processingClient.getLock(HZ_AUDIT_LOCK_NAME);
        }
    }

    public void generateAuditTasks() {
        startHazelcastClient();
        if (auditGenerationLock.tryLock()) {
            try {
                Date auditDate = calculateAuditDate();

                PagedListHolder<ReplicaAuditDto> pagedReplicas = this.auditDao.getReplicasByDate(
                        auditDate, pageSize, 0);

                List<ReplicaAuditDto> replicasToAudit = new ArrayList<ReplicaAuditDto>();
                for (ReplicaAuditDto replicaAudit : pagedReplicas.getPageList()) {
                    Identifier pid = new Identifier();
                    pid.setValue(replicaAudit.getPid());
                    if (!this.processingReplicaAuditIdentifiers.contains(pid)) {
                        this.processingReplicaAuditIdentifiers.add(pid);
                        replicasToAudit.add(replicaAudit);
                    }
                    if (replicasToAudit.size() >= taskChunkSize) {
                        MNAuditTask auditTask = new MNAuditTask(String.valueOf(taskIdGenerator),
                                replicasToAudit);
                        replicaAuditTaskQueue.add(auditTask);
                        replicasToAudit.clear();
                    }
                }
            } finally {
                auditGenerationLock.unlock();
            }
        }
    }

    private Date calculateAuditDate() {
        return null;
    }
}
