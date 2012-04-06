package org.dataone.service.cn.replication.audit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.dataone.configuration.Settings;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.base.RuntimeInterruptedException;

public class ReplicaAuditTaskQueueProcessor implements ItemListener<MNAuditTask> {

    private HazelcastClient processingClient;
    private IQueue<MNAuditTask> replicaAuditTaskQueue;

    private static final String REPLICA_AUDIT_TASK_QUEUE_NAME = Settings.getConfiguration()
            .getString("dataone.hazelcast.replication.audit.task.queue");

    private static Logger log = Logger.getLogger(ReplicaAuditTaskQueueProcessor.class.getName());

    public ReplicaAuditTaskQueueProcessor() {
    }

    public void start() {
        processingClient = Hazelcast.getDefaultInstance();
        replicaAuditTaskQueue = processingClient.getQueue(REPLICA_AUDIT_TASK_QUEUE_NAME);
        replicaAuditTaskQueue.addItemListener(this, true);
    }

    public void stop() {
        if (replicaAuditTaskQueue != null) {
            replicaAuditTaskQueue.removeItemListener(this);
        }
    }

    public void itemAdded(MNAuditTask task) {
        try {
            task = this.replicaAuditTaskQueue.poll(3L, TimeUnit.SECONDS);

            if (task != null) {
                log.info("Submitting replication task id " + task.getTaskid()
                        + " for execution with object identifier: " + task.getPid().getValue()
                        + " on replica node " + task.getTargetNode().getValue());

                ExecutorService executorService = this.processingClient
                        .getExecutorService("ReplicationAuditTasks");
                Future<String> future = executorService.submit(task);

                // check for completion
                boolean isDone = false;
                String result = null;

                while (!isDone) {

                    try {
                        result = (String) future.get(5L, TimeUnit.SECONDS);
                        log.trace("Task result for identifier " + task.getPid().getValue() + " is "
                                + result);
                        if (result != null) {
                            log.debug("Task " + task.getTaskid() + " completed for identifier "
                                    + task.getPid().getValue());

                        }

                    } catch (ExecutionException e) {
                        String msg = e.getCause().getMessage();
                        log.info("MNReplicationTask id " + task.getTaskid()
                                + " threw an execution execption on identifier "
                                + task.getPid().getValue() + ": " + msg);
                        if (task.getRetryCount() < 10) {
                            task.setRetryCount(task.getRetryCount() + 1);
                            future.cancel(true);
                            this.replicaAuditTaskQueue.add(task);
                            log.info("Retrying replication task id " + task.getTaskid()
                                    + " for identifier " + task.getPid().getValue()
                                    + " on replica node " + task.getTargetNode().getValue());

                        } else {
                            log.info("Replication task id" + task.getTaskid()
                                    + " failed, too many retries for identifier"
                                    + task.getPid().getValue() + " on replica node "
                                    + task.getTargetNode().getValue() + ". Not retrying.");
                        }

                    } catch (TimeoutException e) {
                        String msg = e.getMessage();
                        log.info("Replication task id " + task.getTaskid()
                                + " timed out for identifier " + task.getPid().getValue()
                                + " on replica node " + task.getTargetNode().getValue() + " : "
                                + msg);
                        future.cancel(true); // isDone() is now true

                    } catch (InterruptedException e) {
                        String msg = e.getMessage();
                        log.info("Replication task id " + task.getTaskid()
                                + " was interrupted for identifier " + task.getPid().getValue()
                                + " on replica node " + task.getTargetNode().getValue() + " : "
                                + msg);
                        if (task.getRetryCount() < 10) {
                            task.setRetryCount(task.getRetryCount() + 1);
                            this.replicaAuditTaskQueue.add(task);
                            log.info("Retrying replication task id " + task.getTaskid()
                                    + " for identifier " + task.getPid().getValue());

                        } else {
                            log.error("Replication task id" + task.getTaskid()
                                    + " failed, too many retries for identifier"
                                    + task.getPid().getValue() + " on replica node "
                                    + task.getTargetNode().getValue() + ". Not retrying.");
                        }

                    }

                    isDone = future.isDone();
                    log.debug("Task " + task.getTaskid() + " is done for identifier "
                            + task.getPid().getValue() + " on replica node "
                            + task.getTargetNode().getValue() + ": " + isDone);

                    // handle canceled tasks (from the timeout period)
                    if (future.isCancelled()) {
                        log.info("Replication task id " + task.getTaskid()
                                + " was cancelled for identifier " + task.getPid().getValue());

                        // leave the Replica entry as QUEUED in system metadata
                        // to be picked up later
                    }
                }

            }

        } catch (InterruptedException e) {

            String message = "Polling of the replication task queue was interrupted. "
                    + "The message was: " + e.getMessage();
            log.info(message);
        } catch (RuntimeInterruptedException rie) {
            String message = "Hazelcast instance was lost due to cluster shutdown, "
                    + rie.getMessage();

        } catch (IllegalStateException ise) {
            String message = "Hazelcast instance was lost due to cluster shutdown, "
                    + ise.getMessage();
        }

    }

    public void itemRemoved(MNAuditTask item) {
        // no processing needed
    }

}
