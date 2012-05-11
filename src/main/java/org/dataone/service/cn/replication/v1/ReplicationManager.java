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

package org.dataone.service.cn.replication.v1;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.base.RuntimeInterruptedException;

/**
 * A DataONE Coordinating Node implementation which manages replication queues
 * and executes replication tasks. The service is a Hazelcast cluster member and
 * client and listens for queued replication tasks. Queued tasks are popped from
 * the queue on a first come first serve basis, and are executed in a
 * distributed manner (not necessarily on the node that popped the task).
 * 
 * @author cjones
 * 
 */
public class ReplicationManager implements ItemListener<MNReplicationTask> {

    /* Get a Log instance */
    public static Log log = LogFactory.getLog(ReplicationManager.class);

    /* The instance of the Hazelcast storage cluster client */
    private HazelcastClient hzClient;

    /* the instance of the Hazelcast processing cluster member */
    private HazelcastInstance hzMember;

    /*
     * The instance of the IdGenerator created by Hazelcast used to generate
     * "task-ids"
     */
    private IdGenerator taskIdGenerator;

    /* The name of the node map */
    private String nodeMap;

    /* The name of the system metadata map */
    private String systemMetadataMap;

    /* The name of the replication tasks queue */
    private String tasksQueue;

    /* The name of the replication events queue */
    private String eventsQueue;

    /* The Hazelcast distributed task id generator namespace */
    private String taskIds;

    /* The Hazelcast distributed audit lock string name */
    private String hzAuditString;

    /* The Hazelcast distributed audit lock string name */
    private String shortListAge;

    /* The Hazelcast distributed audit lock string name */
    private String shortListNumRows;

    /* The Hazelcast distributed system metadata map */
    private IMap<NodeReference, Node> nodes;

    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadata;

    /* The Hazelcast distributed replication tasks queue */
    private IQueue<MNReplicationTask> replicationTasks;

    /* The Replication task thread queue */
    private BlockingQueue<Runnable> taskThreadQueue;

    /* The handler instance for rejected tasks */
    private RejectedExecutionHandler handler;

    /* The thread pool executor instance for executing tasks */
    private ThreadPoolExecutor executor;

    /* The event listener used to manage incoming map and queue changes */
    private ReplicationEventListener listener;

    /*
     * The timeout period for tasks submitted to the executor service to
     * complete the call to MN.replicate()
     */
    private long timeout = 30L;

    /* A client reference to the coordinating node */
    private CNReplication cnReplication = null;

    /**
     * Constructor - singleton pattern
     */
    public ReplicationManager() {

        this.nodeMap = Settings.getConfiguration().getString("dataone.hazelcast.nodes");
        this.systemMetadataMap = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");
        this.tasksQueue = Settings.getConfiguration().getString(
                "dataone.hazelcast.replicationQueuedTasks");
        this.taskIds = Settings.getConfiguration().getString("dataone.hazelcast.tasksIdGenerator");
        this.hzAuditString = Settings.getConfiguration().getString("dataone.hazelcast.auditString");
        this.shortListAge = Settings.getConfiguration().getString("dataone.hazelcast.shortListAge");
        this.shortListNumRows = Settings.getConfiguration().getString(
                "dataone.hazelcast.shortListNumRows");

        // Connect to the Hazelcast storage cluster
        this.hzClient = HazelcastClientInstance.getHazelcastClient();

        // Connect to the Hazelcast process cluster
        log.info("Becoming a DataONE Process cluster hazelcast member with the default instance.");
        this.hzMember = Hazelcast.getDefaultInstance();

        // get references to cluster structures
        this.nodes = this.hzMember.getMap(nodeMap);
        this.systemMetadata = this.hzClient.getMap(systemMetadataMap);
        this.replicationTasks = this.hzMember.getQueue(tasksQueue);
        this.taskIdGenerator = this.hzMember.getIdGenerator(taskIds);

        // monitor the replication structures

        this.replicationTasks.addItemListener(this, true);
        log.info("Added a listener to the " + this.replicationTasks.getName() + " queue.");

        // Set up the certificate location, create a null session
        String clientCertificateLocation = Settings.getConfiguration().getString(
                "D1Client.certificate.directory")
                + File.separator
                + Settings.getConfiguration().getString("D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        log.info("ReplicationManager is using an X509 certificate from "
                + clientCertificateLocation);

    }

    public void init() {
        log.info("initialization");
        CNode cnode = null;
        // Get an CNode reference to communicate with
        try {
            log.debug("D1Client.CN_URL = "
                    + Settings.getConfiguration().getProperty("D1Client.CN_URL"));

            cnode = D1Client.getCN();
            log.info("ReplicationManager D1Client base_url is: " + cnode.getNodeBaseServiceUrl());
        } catch (ServiceFailure e) {

            // try again, then fail
            try {
                try {
                    Thread.sleep(5000L);

                } catch (InterruptedException e1) {
                    log.error("There was a problem getting a Coordinating Node reference.");
                    e1.printStackTrace();

                }
                cnode = D1Client.getCN();

            } catch (ServiceFailure e1) {
                log.error("There was a problem getting a Coordinating Node reference "
                        + " for the ReplicationManager. ");
                e1.printStackTrace();
                throw new RuntimeException(e1);

            }
        }
        this.cnReplication = cnode;
    }

    /**
     * Create replication tasks given the identifier of an object by evaluating
     * its system metadata and the capabilities of the target replication nodes.
     * Queue the tasks for processing.
     * 
     * @param pid
     *            - the identifier of the object to be replicated
     * @return count - the number of replication tasks queued
     * 
     * @throws ServiceFailure
     * @throws NotImplemented
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotFound
     */
    public int createAndQueueTasks(Identifier pid) throws ServiceFailure, NotImplemented,
            InvalidToken, NotAuthorized, InvalidRequest, NotFound {

        log.info("ReplicationManager.createAndQueueTasks called.");
        boolean allowed;
        int taskCount = 0;
        int desiredReplicas = 3;
        List<Replica> replicaList; // the replica list for this pid
        Set<NodeReference> nodeList; // the full nodes list
        List<NodeReference> potentialNodeList; // the MN subset of the nodes
                                               // list
        Node targetNode = new Node(); // the target node for the replica
        Node authoritativeNode = new Node(); // the source node of the object
        SystemMetadata sysmeta;
        Session session = null;

        // if replication isn't allowed, return
        allowed = isAllowed(pid);
        log.info("Replication is allowed for identifier " + pid.getValue());

        if (!allowed) {
            log.info("Replication is not allowed for the object identified by " + pid.getValue());
            return 0;

        }

        boolean no_task_with_pid = true;

        // check if there are pending tasks or tasks recently put into the task
        // queue
        if (!isPending(pid)) {
            log.debug("Replication is not pending for identifier " + pid.getValue());
            // check for already queued tasks for this pid
            for (MNReplicationTask task : replicationTasks) {
                // if the task's pid is equal to the event's pid
                if (task.getPid().getValue().equals(pid.getValue())) {
                    no_task_with_pid = false;
                    log.debug("An MNReplicationTask is already queued for identifier "
                            + pid.getValue());
                    break;

                }
            }
            if (!no_task_with_pid) {
                log.info("A replication task for the object identified by " + pid.getValue()
                        + " has already been queued.");
                return 0;

            }
        }

        // get the system metadata for the pid
        log.info("Getting the replica list for identifier " + pid.getValue());

        sysmeta = this.systemMetadata.get(pid);
        replicaList = sysmeta.getReplicaList();
        if (replicaList == null) {
            replicaList = new ArrayList<Replica>();

        }
        // List of Nodes for building MNReplicationTasks
        log.info("Building a potential target node list for identifier " + pid.getValue());
        nodeList = (Set<NodeReference>) this.nodes.keySet();
        potentialNodeList = new ArrayList<NodeReference>(); // will be our short
                                                            // list

        // authoritative member node to replicate from
        try {
            authoritativeNode = this.nodes.get(sysmeta.getAuthoritativeMemberNode());

        } catch (NullPointerException npe) {
            throw new InvalidRequest("1080", "Object " + pid.getValue()
                    + " has no authoritative Member Node in its SystemMetadata");

        }

        // build the potential list of target nodes
        for (NodeReference nodeReference : nodeList) {
            Node node = this.nodes.get(nodeReference);

            // only add MNs as targets, excluding the authoritative MN and MNs
            // that are not tagged to replicate
            if ((node.getType() == NodeType.MN)
                    && node.isReplicate()
                    && !node.getIdentifier().getValue()
                            .equals(authoritativeNode.getIdentifier().getValue())) {
                potentialNodeList.add(node.getIdentifier());

            }
        }

        // prioritize replica targets by preferred/blocked lists and other
        // performance metrics
        potentialNodeList = prioritizeNodes(potentialNodeList, sysmeta);

        // parse the sysmeta.ReplicationPolicy
        ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();

        // set the desired replicas if present
        if (replicationPolicy.getNumberReplicas() != null) {
            desiredReplicas = replicationPolicy.getNumberReplicas().intValue();

        }

        log.info("Desired replicas for identifier " + pid.getValue() + " is " + desiredReplicas);
        log.info("Potential target node list size for " + pid.getValue() + " is " + desiredReplicas);

        // can't have more replicas than MNs
        if (desiredReplicas > potentialNodeList.size()) {
            desiredReplicas = potentialNodeList.size(); // yikes
            log.info("Changed the desired replicas for identifier " + pid.getValue()
                    + " to the size of the potential target node list: " + potentialNodeList.size());

        }

        boolean alreadyAdded = false;

        // for each node in the potential node list up to the desired replicas
        for (int j = 0; j < desiredReplicas; j++) {

            NodeReference potentialNode = potentialNodeList.get(j);
            // for each replica in the replica list
            for (Replica replica : replicaList) {

                // ensure that this node is not queued, requested, failed, or
                // completed
                NodeReference replicaNode = replica.getReplicaMemberNode();
                ReplicationStatus status = replica.getReplicationStatus();
                if (potentialNode.getValue().equals(replicaNode.getValue())
                        && (status.equals(ReplicationStatus.QUEUED)
                                || status.equals(ReplicationStatus.REQUESTED)
                                || status.equals(ReplicationStatus.FAILED) || status
                                    .equals(ReplicationStatus.COMPLETED))) {
                    alreadyAdded = true;
                    break;

                }
                log.info("A replication task for identifier " + pid.getValue() + " on node id "
                        + replicaNode.getValue() + " has already been added: " + alreadyAdded
                        + ". The status is " + "currently set to: " + status);

            }
            // if the node doesn't already exist as a pending task for this pid
            if (!alreadyAdded) {
                targetNode = this.nodes.get(potentialNode);

            } else {
                // skip on to the next one right? -rpw (otherwise targetnode is
                // empty or is the last targetnode assigned)
                continue;
            }

            // may be more than one version of MNReplication
            List<String> implementedVersions = new ArrayList<String>();
            List<Service> origServices = authoritativeNode.getServices().getServiceList();
            for (Service service : origServices) {
                if (service.getName().equals("MNReplication") && service.getAvailable()) {
                    implementedVersions.add(service.getVersion());
                }
            }
            if (implementedVersions.isEmpty()) {
                throw new InvalidRequest("1080", "Authoritative Node:"
                        + authoritativeNode.getIdentifier().getValue()
                        + " MNReplication Service is not available or is missing version");
            }

            boolean replicable = false;

            for (Service service : targetNode.getServices().getServiceList()) {
                if (service.getName().equals("MNReplication")
                        && implementedVersions.contains(service.getVersion())
                        && service.getAvailable()) {
                    replicable = true;
                }
            }
            log.info("Based on evaluating the target node services, node id "
                    + targetNode.getIdentifier().getValue() + " is replicable: " + replicable
                    + " (during evaluation for " + pid.getValue() + ")");

            // a replica doesn't exist. add it
            if (replicable) {
                Replica replicaMetadata = new Replica();
                replicaMetadata.setReplicaMemberNode(targetNode.getIdentifier());
                replicaMetadata.setReplicationStatus(ReplicationStatus.QUEUED);
                replicaMetadata.setReplicaVerified(Calendar.getInstance().getTime());

                try {
                    sysmeta = this.systemMetadata.get(pid); // refresh sysmeta
                                                            // to avoid
                                                            // VersionMismatch
                    this.cnReplication.updateReplicationMetadata(session, pid, replicaMetadata,
                            sysmeta.getSerialVersion().longValue());

                } catch (VersionMismatch e) {

                    // retry if the serialVersion is wrong
                    try {
                        sysmeta = this.systemMetadata.get(pid);
                        this.cnReplication.updateReplicationMetadata(session, pid, replicaMetadata,
                                sysmeta.getSerialVersion().longValue());

                    } catch (VersionMismatch e1) {
                        String msg = "Couldn't get the correct serialVersion to update "
                                + "the replica metadata for identifier " + pid.getValue()
                                + " and target node " + targetNode.getIdentifier().getValue();
                        log.info(msg);

                    }

                }

                log.info("Updated system metadata for identifier " + pid.getValue()
                        + " with QUEUED replication status.");

                Long taskid = taskIdGenerator.newId();
                // add the task to the task list
                log.info("Adding a new MNReplicationTask to the queue where " + "pid = "
                        + pid.getValue() + ", originatingNode = "
                        + authoritativeNode.getIdentifier().getValue() + ", targetNode = "
                        + targetNode.getIdentifier().getValue());

                MNReplicationTask task = new MNReplicationTask(taskid.toString(), pid,
                        authoritativeNode.getIdentifier(), targetNode.getIdentifier());
                this.replicationTasks.add(task);
                taskCount++;

            }

        }

        // return the number of replication tasks queued
        log.info("Added " + taskCount + " MNReplicationTasks to the queue for " + pid.getValue());

        return taskCount;

    }

    /**
     * Regular replication sweep over all of the objects on MNs
     * 
     * public void auditReplicas() { try{ // get lock on hzAuditString
     * Hazelcast.getLock(this.hzAuditString);
     * 
     * CNode cn = null;
     * 
     * log.info("Getting the CNode reference for the audit list query."); cn =
     * D1Client.getCN();
     * 
     * // call against the SOLR Indexer to receive a short list of identifiers
     * // which have replicas unchecked in > shortListAge SolrDocumentList
     * shortList = cn.getAuditShortList(this.shortListAge,
     * Integer.parseInt(this.shortListNumRows));
     * 
     * SystemMetadata sysmeta = null;
     * 
     * // bin the tasks by NodeReference for bulk processing by MNAuditTask for
     * (SolrDocument doc : shortList) { sysmeta =
     * this.systemMetadata.get(doc.get("id")); for (Replica replica :
     * sysmeta.getReplicaList()) { if (replica.getReplicaVerified()) { // } else
     * { // } } } } catch (ServiceFailure sf) {
     * log.error("Failed to get the CNode for the audit list query."); } catch
     * (SolrServerException sse) {
     * log.error("Failed to perform query on SOLR Index for audit short list");
     * } }
     */

    /**
     * Implement the ItemListener interface, responding to items being added to
     * the hzReplicationTasks queue.
     * 
     * @param task
     *            - the MNReplicationTask being added to the queue
     */
    public void itemAdded(MNReplicationTask task) {

        // When a task is added to the queue, attempt to handle the task. If
        // successful, execute the task.
        try {
            task = this.replicationTasks.poll(3L, TimeUnit.SECONDS);

            if (task != null) {
                log.info("Submitting replication task id " + task.getTaskid()
                        + " for execution with object identifier: " + task.getPid().getValue()
                        + " on replica node " + task.getTargetNode().getValue());

                // TODO: handle the case when a CN drops and the
                // MNReplicationTask.call()
                ExecutorService executorService = this.hzMember
                        .getExecutorService("ReplicationTasks");
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
                        String msg = "";
                        if (e.getCause() != null) {
                            msg = e.getCause().getMessage();

                        }
                        log.info("MNReplicationTask id " + task.getTaskid()
                                + " threw an execution execption on identifier "
                                + task.getPid().getValue() + ": " + msg);
                        if (task.getRetryCount() < 10) {
                            task.setRetryCount(task.getRetryCount() + 1);
                            future.cancel(true);
                            this.replicationTasks.add(task);
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
                            this.replicationTasks.add(task);
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

    /**
     * Implement the ItemListener interface, responding to items being removed
     * from the hzReplicationTasks queue.
     * 
     * @param task
     *            - the object being removed from the queue (ReplicationTask)
     */
    public void itemRemoved(MNReplicationTask task) {
        // not implemented until needed

    }

    /**
     * Check if replication is allowed for the given pid
     * 
     * @param pid
     *            - the identifier of the object to check
     * @return
     */
    public boolean isAllowed(Identifier pid) {

        log.debug("ReplicationManager.isAllowed() called for " + pid.getValue());
        boolean isAllowed = false;
        SystemMetadata sysmeta = null;
        ReplicationPolicy policy = null;

        try {
            sysmeta = this.systemMetadata.get(pid);
            policy = sysmeta.getReplicationPolicy();
            isAllowed = policy.getReplicationAllowed().booleanValue();

        } catch (NullPointerException e) {
            log.error("NullPointerException caught in ReplicationManager.isAllowed() "
                    + "for identifier " + pid.getValue());
            log.debug("ReplicationManager.isAllowed() = " + isAllowed + " for " + pid.getValue());
            isAllowed = false;

        } catch (RuntimeException re) {
            log.error("Runtime exception caught in ReplicationManager.isAllowed() "
                    + "for identifier " + pid.getValue() + ". The error message was: "
                    + re.getMessage());
            log.debug("ReplicationManager.isAllowed() = " + isAllowed + " for " + pid.getValue());
            isAllowed = false;
        }

        return isAllowed;
    }

    /*
     * Check to see if replication tasks are pending for the given pid
     * 
     * @param pid - the identifier of the object to check
     */
    public boolean isPending(Identifier pid) {
        SystemMetadata sysmeta = this.systemMetadata.get(pid);
        List<Replica> replicaList = sysmeta.getReplicaList();
        boolean is_pending = false;

        // do we have pending replicas for this pid?
        for (Replica replica : replicaList) {
            ReplicationStatus status = replica.getReplicationStatus();
            if (status.equals(ReplicationStatus.QUEUED)
                    || status.equals(ReplicationStatus.REQUESTED)) {
                is_pending = true;
                break;

            }

        }

        return is_pending;
    }

    public void setCnReplication(CNReplication cnReplication) {
        this.cnReplication = cnReplication;
    }

    /**
     * For the given node list, report the pending request factor of each node.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return requestFactors the pending request factors of the nodes
     */
    public Map<NodeReference, Float> getPendingRequestFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {

        // TODO: implement the useCache parameter, ignored for now

        // A map to store the raw pending replica counts
        Map<NodeReference, Integer> pendingRequests = new HashMap<NodeReference, Integer>();
        // A map to store the current request factors per node
        Map<NodeReference, Float> requestFactors = new HashMap<NodeReference, Float>();

        /*
         * See http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Replication Requests Factor R ----------------------------- The goal
         * here is to be sure not to overload nodes by only issuing a fixed
         * number of requests for replication to a given member node. If the
         * request limit is reached, don't submit more requests.
         * 
         * Max request limit (rl) Number of pending replication tasks on target
         * (rt)
         * 
         * R = 1 if rt < rl, 0 otherwise Also want to deal wiht the number of
         * requests pending against a source node, but defer until later:
         * 
         * Number of pending replication tasks on source (rs) To be determined
         * -- refactor R including rs
         */

        // TODO: Use a configurable limit. For now, define a static request
        // limit
        int requestLimit = 10;

        // query the systemmetadatastatus table to get counts of queued and
        // requested replicas by node identifier
        try {
            pendingRequests = DaoFactory.getReplicationDao().getPendingReplicasByNode();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve pending replicas by node: " + dataAccessEx.getMessage());
        }
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        // determine results for each MN in the list
        while (nodeIterator.hasNext()) {
            NodeReference nodeId = nodeIterator.next();

            // get the failures for the node
            Integer pending = 
                (pendingRequests.get(nodeId) != null) ? pendingRequests.get(nodeId): new Integer(0);
            log.debug("Pending requests for node " + nodeId.getValue() + " is " +
                    pending.intValue());

            if (pending.intValue() <= requestLimit) {
                // currently under or equal to the limit
                requestFactors.put(nodeId, new Float(1));

            } else {
                // currently over the limit
                requestFactors.put(nodeId, new Float(0));
                log.info("Node " + nodeId.getValue() + " is currently over its request limit of "
                        + requestLimit + " requests.");

            }

        }
        return requestFactors;
    }

    /**
     * For the given node list, report the success factor as a surrogate for the
     * nodes' demonstrated replication successes over a recent time period.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return failureFactors the failure factors of the nodes
     */
    public Map<NodeReference, Float> getFailureFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {
        // A map to store the raw failed replica counts
        Map<NodeReference, Integer> failedRequests = new HashMap<NodeReference, Integer>();
        // A map to store the raw completed replica counts
        Map<NodeReference, Integer> completedRequests = new HashMap<NodeReference, Integer>();
        // A map to store the current failure factors per node
        HashMap<NodeReference, Float> failureFactors = new HashMap<NodeReference, Float>();
        Float successThreshold = new Float(0.8f);

        /*
         * See http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Failure Factor F ---------------- The goal here is to avoid nodes
         * that are failing a lot, and for those that are failing less than an
         * arbitrary threshold, prioritize them proportionally to their success
         * rate.
         * 
         * Number of replication successes over last 3 days (ps) Number of
         * replication failures over last 3 days (pf) days)
         * 
         * Success threshold (st) = default 0.80 F = 0 if ps/(ps+pf) <= st, else
         * ps/(ps+pf)
         */
        try {
            failedRequests = DaoFactory.getReplicationDao().getRecentFailedReplicas();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve recent failed replicas by node: "
                    + dataAccessEx.getMessage());
        }
        try {
            completedRequests = DaoFactory.getReplicationDao().getRecentCompletedReplicas();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve recent completed replicas by node: "
                    + dataAccessEx.getMessage());
        }
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        while (nodeIterator.hasNext()) {
            NodeReference nodeId = nodeIterator.next();

            // get the failures for the node
            Integer failures = (failedRequests.get(nodeId) != null) ? failedRequests.get(nodeId)
                    : new Integer(0);
            // get the successes for the node
            Integer successes = (completedRequests.get(nodeId) != null) ? completedRequests
                    .get(nodeId) : new Integer(0);

            // in the case there's no real stats
            if (failures.intValue() == 0 && successes.intValue() == 0) {
                // bootstrap the MN as a medium-performant node
                failureFactors.put((NodeReference) nodeIterator.next(), new Float(0.9f));

            } else {
                // calculate the failure factor
                Float failureFactor = new Float(successes.floatValue()
                        / (successes.floatValue() + failures.floatValue()));
                if (failureFactor <= successThreshold) {
                    failureFactor = new Float(0.0f);
                }
                failureFactors.put((NodeReference) nodeIterator.next(), failureFactor);

            }

        }

        return failureFactors;
    }

    /**
     * For the given nodes, return the bandwidth factor as a surrogate for the
     * nodes' demonstrated throughput over a recent time period.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return bandwidthFactors the bandwidth factor of the node
     */
    public Map<NodeReference, Float> getBandwidthFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {
        HashMap<NodeReference, Float> bandwidthFactors = new HashMap<NodeReference, Float>();

        /*
         * TODO: calculate the bandwidth factor based on the following notes at
         * http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Bandwith Factor B ----------------- The goal here is to utilize high
         * bandwidth nodes more than low by skewing the rank in favor of high
         * bandwidth nodes. We do this by calculating B from 0 to 2 and
         * multiplying the other metrics by B, which will proportionally reduce
         * or enhance the rank based on B. THe metric following uses the range
         * of bandwidths available across all nodes to determine B such that the
         * lowest bandwidth nodes will be near zero and the highest near 2, but
         * a lot of the nodes will cluster around 1 due to the log functions.
         * 
         * B = 2*(log(b/bmin) / log(bmax/bmin))
         * 
         * will range from 0 to 2 Node Bandwidth b MaxNodeBandwith bmax
         * MinNodeBandwidth bmin
         * 
         * Note that its not clear how we actually estimate node bandwidth -- is
         * it a node reported metadata value, or something we measure during
         * normal operations? The latter would be possible by recording the time
         * to replicate data between two nodes and dividing by the replica size,
         * and assign the resultant value to both nodes -- over time an average
         * would build up indicating the effective throughput that considers not
         * just network bandwidth but also storage I/O rates and admin overhead.
         */

        // Placeholder code: assign equal bandwidth factors for now
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        while (nodeIterator.hasNext()) {
            bandwidthFactors.put((NodeReference) nodeIterator.next(), new Float(1.0f));
        }

        return bandwidthFactors;
    }

    /**
     * Prioritize a list of potential replica target nodes based on a number of
     * factors including preferred/blocked node lists, pending request, failure,
     * and bandwidth factors.
     * 
     * @param sysmeta
     * @param potentialNodeList
     * 
     * @return nodesByPriority a list of nodes by descending priority
     */
    public List<NodeReference> prioritizeNodes(List<NodeReference> potentialNodeList,
            SystemMetadata sysmeta) {
        List<NodeReference> nodesByPriority = new ArrayList<NodeReference>();
        ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();
        Identifier pid = sysmeta.getIdentifier();
        Map<NodeReference, Float> requestFactorMap = new HashMap<NodeReference, Float>();
        Map<NodeReference, Float> failureFactorMap = new HashMap<NodeReference, Float>();
        Map<NodeReference, Float> bandwidthFactorMap = new HashMap<NodeReference, Float>();

        log.info("Retrieving performance metrics for the potential replication list for "
                + pid.getValue());

        // get performance metrics for the potential node list
        requestFactorMap = getPendingRequestFactors(potentialNodeList, false);
        failureFactorMap = getFailureFactors(potentialNodeList, false);
        bandwidthFactorMap = getBandwidthFactors(potentialNodeList, false);

        // get the preferred list, if any
        List<NodeReference> preferredList = null;
        if (replicationPolicy != null) {
            preferredList = replicationPolicy.getPreferredMemberNodeList();

        }

        // get the blocked list, if any
        List<NodeReference> blockedList = null;
        if (replicationPolicy != null) {
            preferredList = replicationPolicy.getBlockedMemberNodeList();

        }

        Map<NodeReference, Float> nodeScoreMap = new HashMap<NodeReference, Float>();
        SortedSet<Map.Entry<NodeReference, Float>> sortedScores;
        Iterator<NodeReference> nodeIterator = potentialNodeList.iterator();

        // iterate through the potential node list and calculate performance
        // scores
        while (nodeIterator.hasNext()) {
            NodeReference nodeId = (NodeReference) nodeIterator.next();
            Float preferenceFactor = 1.0f; // default preference for all nodes

            // increase preference for preferred nodes
            if (preferredList != null && preferredList.contains(nodeId)) {
                preferenceFactor = 2.0f;

            }

            // decrease preference for preferred nodes
            if (blockedList != null && blockedList.contains(nodeId)) {
                preferenceFactor = 0.0f;


            }
            log.debug("Node " + nodeId.getValue() + " preferenceFactor is " + preferenceFactor);

            Float nodePendingRequestFactor = null;
            if ( requestFactorMap.get(nodeId) != null) { 
                nodePendingRequestFactor = requestFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " requestFactor is " + nodePendingRequestFactor);
                
            } else {
                nodePendingRequestFactor = 0.0f;
            }
            
            Float nodeFailureFactor = null;
            if (failureFactorMap.get(nodeId) != null ) {
                nodeFailureFactor = failureFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " failureFactor is " + nodeFailureFactor);

            } else {
                nodeFailureFactor = 0.0f;
                
            }
            
            Float nodeBandwidthFactor = null;
            if (bandwidthFactorMap.get(nodeId) != null ) {
                nodeBandwidthFactor = bandwidthFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " bandwidthFactor is " + nodeBandwidthFactor);

            } else {
                nodeBandwidthFactor = 0.0f;
                
            }

            // Score S = R * F * B * P 
            // (any zero score removes node from the list)
            Float score = nodePendingRequestFactor * 
                          nodeFailureFactor        * 
                          nodeBandwidthFactor      * 
                          preferenceFactor;
            log.debug("Score for " + nodeId.getValue()        + " will be "  +  
                                     nodePendingRequestFactor + " * "        +
                                     nodeFailureFactor        + " * "        +
                                     nodeBandwidthFactor      + " * "        +
                                     preferenceFactor);
            log.info("Priority score for " + nodeId.getValue() + " is " + score.floatValue());
            nodeScoreMap.put(nodeId, score);

            // remove blocked and non-performant nodes
            Iterator<Map.Entry<NodeReference, Float>> iterator = 
                nodeScoreMap.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<NodeReference, Float> entry = iterator.next();
                if (entry.getValue().intValue() == 0) {
                    nodeScoreMap.remove(entry.getKey());

                }
            }
        }

        sortedScores = entriesSortedByValues(nodeScoreMap);
        Iterator<Entry<NodeReference, Float>> scoresIterator = sortedScores.iterator();

        if (log.isDebugEnabled()) {
            log.debug("Prioritized nodes list: ");
        }

        // fill the list according to priority by adding each successively less
        // prioritized nodeId to the end of the list
        while (scoresIterator.hasNext()) {
            Entry<NodeReference, Float> entry = scoresIterator.next();
            NodeReference n = entry.getKey();
            Float s = entry.getValue();
            nodesByPriority.add(nodesByPriority.size(), n);
            if (log.isDebugEnabled()) {
                log.debug("Node:\t" + n.getValue() + ", score:\t" + s.intValue());
            }
        }

        return nodesByPriority;
    }

    /*
     * A generic method to sort a map by the values. Used to prioritize nodes.
     * 
     * @param map the map to sort
     * 
     * @return sortedEntries the sorted set of map entries
     */
    private <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(
            Map<K, V> map) {

        SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(
                new Comparator<Map.Entry<K, V>>() {
                    @Override
                    public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
                        int res = e1.getValue().compareTo(e2.getValue());
                        return res != 0 ? res : 1; // preserve items with equal
                                                   // values
                    }
                });
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

}
