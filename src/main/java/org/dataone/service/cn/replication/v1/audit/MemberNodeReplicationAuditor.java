/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dataone.service.cn.replication.v1.audit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.types.v1.Identifier;

import com.hazelcast.client.HazelcastClient;

/**
 * 
 * @author sroseboo
 * 
 */
public class MemberNodeReplicationAuditor implements Runnable {

    private static Logger log = Logger.getLogger(MemberNodeReplicationAuditor.class.getName());

    private static final int pageSize = 100;
    private static final int pidsPerTaskSize = 10;
    private static final int taskPoolSize = 10;
    private static final int maxPages = 1000;
    private static final long auditPeriod = 1000 * 60 * 60 * 24 * 14; // 14 days

    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();
    private HazelcastClient processingClient;
    private Lock auditLock;
    private ExecutorService executorService;

    private static final String MN_AUDIT_LOCK_NAME = "memberNodeReplicationAuditLock";

    public MemberNodeReplicationAuditor() {
        this.processingClient = HazelcastClientFactory.getProcessingClient();
        this.executorService = Executors.newFixedThreadPool(taskPoolSize);
    }

    @Override
    public void run() {
        auditReplication();
    }

    public void auditReplication() {
        if (ComponentActivationUtility.replicationIsActive()) {
            auditLock = processingClient.getLock(MN_AUDIT_LOCK_NAME);
            try {
                if (auditLock.tryLock()) {
                    Date auditDate = calculateAuditDate();
                    List<Identifier> pidsToAudit = null;
                    for (int i = 1; i < maxPages; i++) {
                        try {
                            pidsToAudit = this.replicationDao.getReplicasByDate(auditDate, i,
                                    pageSize);
                        } catch (DataAccessException dae) {
                            log.error(
                                    "Unable to retrieve replicas by date using replication dao for audit date: "
                                            + auditDate.toString() + ".", dae);
                        }
                        if (pidsToAudit.size() == 0) {
                            break;
                        }
                        auditPids(pidsToAudit, auditDate);
                    }
                }
            } finally {
                auditLock.unlock();
            }
        }
    }

    private void auditPids(List<Identifier> pids, Date auditDate) {
        List<Identifier> pidBatch = new ArrayList<Identifier>();
        List<MemberNodeReplicaAuditTask> auditTaskBatch = new ArrayList<MemberNodeReplicaAuditTask>();
        List<Future> currentFutures = new ArrayList<Future>();
        List<Future> previousFutures = new ArrayList<Future>();

        for (Identifier pid : pids) {
            pidBatch.add(pid);
            if (pidBatch.size() >= pidsPerTaskSize) {
                auditTaskBatch.add(new MemberNodeReplicaAuditTask(pidBatch, auditDate));
                pidBatch.clear();
            }
            if (auditTaskBatch.size() >= taskPoolSize) {
                submitTasks(auditTaskBatch, currentFutures, previousFutures);
            }
            if (!previousFutures.isEmpty()) {
                handleFutures(previousFutures);
            }
        }
        handleFutures(currentFutures);
    }

    private void submitTasks(List<MemberNodeReplicaAuditTask> tasks, List<Future> currentFutures,
            List<Future> previousFutures) {

        previousFutures.clear();
        previousFutures.addAll(currentFutures);
        currentFutures.clear();
        for (MemberNodeReplicaAuditTask auditTask : tasks) {
            submitTask(currentFutures, auditTask);
        }
        tasks.clear();
    }

    private void submitTask(List<Future> currentFutures, MemberNodeReplicaAuditTask auditTask) {
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
}
