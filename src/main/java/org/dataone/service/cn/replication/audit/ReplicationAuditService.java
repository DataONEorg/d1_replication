package org.dataone.service.cn.replication.audit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IdGenerator;

public class ReplicationAuditService {

    private static final int pageSize = 0;
    private static final int pidChunkSize = 1;
    private static final int taskChunkSize = 10;
    private static final long auditPeriod = 1000 * 60 * 60 * 24 * 14; // 14 days

    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();

    private HazelcastClient processingClient;

    private Set<Identifier> processingReplicaAuditIdentifiers;

    private Lock auditGenerationLock;

    // DOES THIS NEED TO BE DEFINED IN HZCONFIG??????
    private IdGenerator taskIdGenerator;

    private ExecutorService executorService;

    private static final String HZ_AUDIT_LOCK_NAME = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.generation.lock");

    private static final String REPLICA_AUDIT_SET_HANDLED = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.processing.identifiers");

    private static final String REPLICA_AUDIT_TASK_IDS = Settings.getConfiguration().getString(
            "dataone.hazelcast.replication.audit.task.ids");

    private static final String REPLICA_EXECUTOR_SERVICE = "ReplicationAuditTasks";

    private static Logger log = Logger.getLogger(ReplicationAuditService.class.getName());

    public ReplicationAuditService() {
    }

    public static void main(String[] args) {
        ReplicationAuditService service = new ReplicationAuditService();
        service.auditReplication();
    }

    public void auditReplication() {
        configureHazelcast();
        if (auditGenerationLock.tryLock()) {
            try {
                Date auditDate = calculateAuditDate();
                // TODO: implement paging through pids to be audited here
                List<Identifier> pagedReplicas = null;
                try {
                    this.replicationDao.getReplicasByDate(auditDate, pageSize, 0);
                } catch (DataAccessException dae) {
                    log.error(
                            "Unable to retrieve replicas by date using replication dao for audit date: "
                                    + auditDate.toString() + ".", dae);
                }
                auditPids(pagedReplicas);
            } finally {
                auditGenerationLock.unlock();
            }
        }
    }

    private void auditPids(List<Identifier> pids) {
        List<Identifier> pidBatch = new ArrayList<Identifier>();
        List<ReplicationAuditTask> auditTaskBatch = new ArrayList<ReplicationAuditTask>();
        List<Future> currentFutures = new ArrayList<Future>();
        List<Future> previousFutures = new ArrayList<Future>();

        for (Identifier pid : pids) {
            if (!this.processingReplicaAuditIdentifiers.contains(pid)) {
                this.processingReplicaAuditIdentifiers.add(pid);
                pidBatch.add(pid);
            }
            if (pidBatch.size() >= pidChunkSize) {
                auditTaskBatch.add(new ReplicationAuditTask(String.valueOf(taskIdGenerator),
                        pidBatch));
                pidBatch.clear();
            }
            if (auditTaskBatch.size() >= taskChunkSize) {
                submitTasks(auditTaskBatch, currentFutures, previousFutures);
            }
            if (!previousFutures.isEmpty()) {
                handleFutures(previousFutures);
            }
        }
        handleFutures(currentFutures);
    }

    private void submitTasks(List<ReplicationAuditTask> tasks, List<Future> currentFutures,
            List<Future> previousFutures) {

        previousFutures.clear();
        previousFutures.addAll(currentFutures);
        currentFutures.clear();
        for (ReplicationAuditTask auditTask : tasks) {
            submitTask(currentFutures, auditTask);
        }
        tasks.clear();
    }

    private void submitTask(List<Future> currentFutures, ReplicationAuditTask auditTask) {
        Future future = null;
        try {
            future = executorService.submit(auditTask);
        } catch (RejectedExecutionException rej) {
            log.error("Unable to submit tasks to executor service. ", rej);
            log.error("Sleeping for 5 seconds, trying again");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error("sleep interrupted.", e);
            }
            try {
                future = executorService.submit(auditTask);
            } catch (RejectedExecutionException reEx) {
                log.error("Still unable to submit tasks to executor service, failing. ", reEx);
            }
        }
        if (future != null) {
            currentFutures.add(future);
        }
        this.processingReplicaAuditIdentifiers.removeAll(auditTask.getPidsToAudit());
    }

    private void handleFutures(List<Future> taskFutures) {
        for (Future future : taskFutures) {
            handleFuture(future);
        }
    }

    private void handleFuture(Future future) {
        boolean isDone = false;
        String result = null;
        boolean timedOut = false;
        while (!isDone) {
            try {
                result = (String) future.get(5, TimeUnit.SECONDS);
                if (result != null) {
                    log.debug("Replica audit task completed with result: " + result);
                }
            } catch (InterruptedException e) {
                log.error("Replica audit task interrupted, cancelling.", e);
                future.cancel(true);
            } catch (CancellationException e) {
                log.error("Replica audit task cancelled.", e);
            } catch (ExecutionException e) {
                log.error("Replica audit task threw exception during execution. ", e);
            } catch (TimeoutException e) {
                if (timedOut == false) {
                    log.debug("Replica audit task timed out.  waiting another 5 seconds.");
                    timedOut = true;
                } else {
                    log.error("Replica audit task timed out twice, cancelling.");
                    future.cancel(true);
                }
            }
            isDone = future.isDone();
        }
    }

    private Date calculateAuditDate() {
        return new Date(System.currentTimeMillis() - auditPeriod);
    }

    private void configureHazelcast() {
        if (this.processingClient == null) {
            this.processingClient = HazelcastClientFactory.getProcessingClient();
            processingReplicaAuditIdentifiers = processingClient.getSet(REPLICA_AUDIT_SET_HANDLED);
            taskIdGenerator = processingClient.getIdGenerator(REPLICA_AUDIT_TASK_IDS);
            auditGenerationLock = processingClient.getLock(HZ_AUDIT_LOCK_NAME);
            executorService = processingClient.getExecutorService(REPLICA_EXECUTOR_SERVICE);
        }
    }
}
