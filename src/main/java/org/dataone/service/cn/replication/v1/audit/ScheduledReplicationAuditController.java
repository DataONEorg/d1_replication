package org.dataone.service.cn.replication.v1.audit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Controller responsible for scheduling(starting) and stopping replication auditors.
 * 
 * @author sroseboo
 *
 */
public class ScheduledReplicationAuditController {

    private static Log logger = LogFactory.getLog(ScheduledReplicationAuditController.class
            .getName());

    private static ScheduledExecutorService staleRequestedReplicaAuditScheduler;
    private static ScheduledExecutorService staleQueuedReplicaAuditScheduler;
    private static ScheduledExecutorService mnReplicaAuditScheduler;

    public ScheduledReplicationAuditController() {

    }

    public void startup() {
        logger.info("starting scheduled replication auditing...");
        startStaleRequestedAuditing();
        startStaleQueuedAuditing();
        // startMnReplicaAuditing();
        logger.info("scheduled replication auditing started.");
    }

    public void shutdown() {
        logger.info("stopping scheduled replication auditing....");
        if (staleRequestedReplicaAuditScheduler != null) {
            staleRequestedReplicaAuditScheduler.shutdown();
        }
        if (staleQueuedReplicaAuditScheduler != null) {
            staleQueuedReplicaAuditScheduler.shutdown();
        }
        if (mnReplicaAuditScheduler != null) {
            mnReplicaAuditScheduler.shutdown();
        }
        logger.info("scheduled replication auditing stopped.");
    }

    private void startStaleQueuedAuditing() {
        if (staleQueuedReplicaAuditScheduler == null
                || staleQueuedReplicaAuditScheduler.isShutdown()) {
            staleQueuedReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            staleQueuedReplicaAuditScheduler.scheduleAtFixedRate(new QueuedReplicationAuditor(),
                    0L, 1L, TimeUnit.HOURS);
        }
    }

    private void startStaleRequestedAuditing() {
        if (staleRequestedReplicaAuditScheduler == null
                || staleRequestedReplicaAuditScheduler.isShutdown()) {
            staleRequestedReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            staleRequestedReplicaAuditScheduler.scheduleAtFixedRate(
                    new StaleReplicationRequestAuditor(), 0L, 1L, TimeUnit.HOURS);
        }
    }

    private void startMnReplicaAuditing() {
        if (mnReplicaAuditScheduler == null || mnReplicaAuditScheduler.isShutdown()) {
            mnReplicaAuditScheduler = Executors.newSingleThreadScheduledExecutor();
            mnReplicaAuditScheduler.scheduleAtFixedRate(new MemberNodeReplicationAuditor(), 0L, 1L,
                    TimeUnit.HOURS);
        }
    }
}
