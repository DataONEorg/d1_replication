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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.BaseException;
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
import com.hazelcast.core.ILock;
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

    /* The Hazelcast distributed replication events queue */
    private IQueue<Identifier> replicationEvents;

    /* The event listener used to manage incoming map and queue changes */
    private ReplicationEventListener listener;

    /* The lock on an identifier across the hzProcess cluster */
    private ILock lock;
    /*
     * The timeout period for tasks submitted to the executor service to
     * complete the call to MN.replicate()
     */
    private long timeout = 30L;

    /* A client reference to the coordinating node */
    private CNReplication cnReplication = null;

    /* The prioritization strategy for determining target replica nodes */
    private ReplicationPrioritizationStrategy prioritizationStrategy = new ReplicationPrioritizationStrategy();

    /* The router hostname for the CN cluster (round robin) */
    private String cnRouterHostname;
    
    /**
     * Constructor - singleton pattern
     */
    public ReplicationManager() {

        this.nodeMap = Settings.getConfiguration().getString(
                "dataone.hazelcast.nodes");
        this.systemMetadataMap = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");
        this.tasksQueue = Settings.getConfiguration().getString(
                "dataone.hazelcast.replicationQueuedTasks");
        this.eventsQueue = Settings.getConfiguration().getString(
                "dataone.hazelcast.replicationQueuedEvents");
        this.taskIds = Settings.getConfiguration().getString(
                "dataone.hazelcast.tasksIdGenerator");
        this.hzAuditString = Settings.getConfiguration().getString(
                "dataone.hazelcast.auditString");
        this.shortListAge = Settings.getConfiguration().getString(
                "dataone.hazelcast.shortListAge");
        this.shortListNumRows = Settings.getConfiguration().getString(
                "dataone.hazelcast.shortListNumRows");

        this.cnRouterHostname = 
            Settings.getConfiguration().getString("cn.router.hostname");
        
        // Connect to the Hazelcast storage cluster
        this.hzClient = HazelcastClientInstance.getHazelcastClient();

        // Connect to the Hazelcast process cluster
        log.info("Becoming a DataONE Process cluster hazelcast member with the default instance.");
        this.hzMember = Hazelcast.getDefaultInstance();

        // get references to cluster structures
        this.nodes = this.hzMember.getMap(this.nodeMap);
        this.systemMetadata = this.hzClient.getMap(this.systemMetadataMap);
        this.replicationEvents = this.hzMember.getQueue(eventsQueue);
        this.replicationTasks = this.hzMember.getQueue(this.tasksQueue);
        this.taskIdGenerator = this.hzMember.getIdGenerator(this.taskIds);

        // monitor the replication structures

        this.replicationTasks.addItemListener(this, true);
        log.info("Added a listener to the " + this.replicationTasks.getName()
                + " queue.");

        // Set up the certificate location, create a null session
        String clientCertificateLocation = Settings.getConfiguration().getString(
                "D1Client.certificate.directory")
                + File.separator
                + Settings.getConfiguration().getString(
                        "D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(
                clientCertificateLocation);
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
            log.info("ReplicationManager D1Client base_url is: "
                    + cnode.getNodeBaseServiceUrl());
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
    public int createAndQueueTasks(Identifier pid) throws 
            NotImplemented, InvalidRequest {

        log.debug("ReplicationManager.createAndQueueTasks called.");
        boolean allowed;
        int taskCount = 0;
        int desiredReplicas = 3;
        long timeToWait = 5L;
        List<Replica> replicaList; // the replica list for this pid
        Set<NodeReference> nodeList; // the full nodes list
        List<NodeReference> potentialNodeList; // the MN subset of the nodes
                                               // list
        Node targetNode = new Node(); // the target node for the replica
        Node authoritativeNode = new Node(); // the source node of the object
        SystemMetadata sysmeta;
        Session session = null;

        // use the distributed lock
        this.lock = null;
        String lockPid = pid.getValue();
        boolean isLocked = false;
        try {
            isLocked = this.lock.tryLock(timeToWait, TimeUnit.SECONDS);
            // only evaluate an identifier if we can get the lock in a reasonable time
            if ( isLocked ) {
                // if replication isn't allowed, return
                allowed = isAllowed(pid);
                log.debug("Replication is allowed for identifier " + pid.getValue());
                if (!allowed) {
                    log.debug("Replication is not allowed for the object identified by "
                            + pid.getValue());
                    return 0;

                }
                boolean no_task_with_pid = true;
                // check if there are pending tasks or tasks recently put into the task
                // queue
                if (!isPending(pid)) {
                    log.debug("Replication is not pending for identifier "
                            + pid.getValue());
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
                        log.debug("A replication task for the object identified by "
                                + pid.getValue() + " has already been queued.");
                        return 0;

                    }
                }
                // get the system metadata for the pid
                log.debug("Getting the replica list for identifier "
                        + pid.getValue());
                sysmeta = this.systemMetadata.get(pid);
                replicaList = sysmeta.getReplicaList();
                if (replicaList == null) {
                    replicaList = new ArrayList<Replica>();

                }
                // change the desired replicas count to account for pending/completed
                // replicas
                int currentListedReplicaCount = 0;
                List<NodeReference> listedReplicaNodes = new ArrayList<NodeReference>();
                for (Replica listedReplica : replicaList) {
                    NodeReference nodeId = listedReplica.getReplicaMemberNode();
                    ReplicationStatus listedStatus = listedReplica
                            .getReplicationStatus();

                    Node node = new Node();
                    NodeType nodeType = null;
                    try {
                        node = this.nodes.get(nodeId);
                        if (node != null) {
                            log.debug("The potential target node id is: "
                                    + node.getIdentifier().getValue());
                            nodeType = node.getType();
                            if (nodeType == NodeType.CN
                                    || nodeId.getValue().equals(
                                            sysmeta.getAuthoritativeMemberNode()
                                                    .getValue())) {
                                continue; // don't count CNs or authoritative nodes as replicas

                            }

                        } else {
                            log.debug("The potential target node id is null for "
                                    + nodeId.getValue());
                            continue;
                        }

                    } catch (Exception e) {
                        log.debug("There was an error getting the node type: "
                                + e.getMessage());
                        e.printStackTrace();
                    }

                    if (listedStatus == ReplicationStatus.QUEUED
                            || listedStatus == ReplicationStatus.REQUESTED
                            || listedStatus == ReplicationStatus.COMPLETED) {
                        currentListedReplicaCount++;
                        listedReplicaNodes.add(nodeId);
                    }
                }
                log.debug("There are currently " + currentListedReplicaCount
                        + " pending/completed replicas listed for identifier "
                        + pid.getValue());
                // List of Nodes for building MNReplicationTasks
                log.debug("Building a potential target node list for identifier "
                        + pid.getValue());
                nodeList = (Set<NodeReference>) this.nodes.keySet();
                potentialNodeList = new ArrayList<NodeReference>(); // will be our short
                                                                    // list
                // authoritative member node to replicate from
                try {
                    authoritativeNode = this.nodes.get(sysmeta
                            .getAuthoritativeMemberNode());

                } catch (NullPointerException npe) {
                    throw new InvalidRequest( "1080", "Object " + pid.getValue() + 
                            " has no authoritative Member Node in its SystemMetadata");

                } finally {
                    if ( isLocked ) {
                        this.lock.unlock();
                    }
                }
                // build the potential list of target nodes
                for (NodeReference nodeReference : nodeList) {
                    Node node = this.nodes.get(nodeReference);

                    // only add MNs as targets, excluding the authoritative MN and MNs
                    // that are not tagged to replicate
                    if ((node.getType() == NodeType.MN)
                            && node.isReplicate()
                            && !node.getIdentifier()
                                    .getValue()
                                    .equals(authoritativeNode.getIdentifier()
                                            .getValue())) {
                        potentialNodeList.add(node.getIdentifier());

                    }
                }
                potentialNodeList.removeAll(listedReplicaNodes);
                // prioritize replica targets by preferred/blocked lists and other
                // performance metrics
                log.trace("METRICS:\tPRIORITIZE:\tPID:\t" + pid.getValue());
                potentialNodeList = prioritizeNodes(potentialNodeList, sysmeta);
                log.trace("METRICS:\tEND PRIORITIZE:\tPID:\t" + pid.getValue());
                // parse the sysmeta.ReplicationPolicy
                ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();
                // set the desired replicas if present
                if (replicationPolicy.getNumberReplicas() != null) {
                    desiredReplicas = replicationPolicy.getNumberReplicas()
                            .intValue();

                }
                log.info("Desired replicas for identifier " + pid.getValue()
                        + " is " + desiredReplicas);
                log.info("Potential target node list size for " + pid.getValue()
                        + " is " + potentialNodeList.size());
                // can't have more replicas than MNs
                if (desiredReplicas > potentialNodeList.size()) {
                    desiredReplicas = potentialNodeList.size(); // yikes
                    log.info("Changed the desired replicas for identifier "
                            + pid.getValue()
                            + " to the size of the potential target node list: "
                            + potentialNodeList.size());

                }
                // for each node in the potential node list up to the desired replicas
                // (less the pending/completed replicas)
                int desiredReplicasLessListed = 
                    desiredReplicas - currentListedReplicaCount;
                log.debug("Desired replica count less already listed replica count is "
                        + desiredReplicasLessListed);
                for (int j = 0; j < desiredReplicas - currentListedReplicaCount; j++) {

                    log.debug("Evaluating item " + j + " of "
                            + desiredReplicasLessListed
                            + " in the potential node list.");
                    NodeReference potentialNode = potentialNodeList.get(j);

                    targetNode = this.nodes.get(potentialNode);
                    log.debug("currently evaluating "
                            + targetNode.getIdentifier().getValue()
                            + " for task creation " + "for identifier "
                            + pid.getValue());

                    // may be more than one version of MNReplication
                    List<String> implementedVersions = new ArrayList<String>();
                    List<Service> origServices = authoritativeNode.getServices()
                            .getServiceList();
                    for (Service service : origServices) {
                        if (service.getName().equals("MNReplication")
                                && service.getAvailable()) {
                            implementedVersions.add(service.getVersion());
                        }
                    }
                    if (implementedVersions.isEmpty()) {
                        continue; // this node is not replicable
                    }

                    boolean replicable = false;

                    for (Service service : targetNode.getServices().getServiceList()) {
                        if (service.getName().equals("MNReplication")
                                && implementedVersions
                                        .contains(service.getVersion())
                                && service.getAvailable()) {
                            replicable = true;
                        }
                    }
                    log.debug("Based on evaluating the target node services, node id "
                            + targetNode.getIdentifier().getValue()
                            + " is replicable: "
                            + replicable
                            + " (during evaluation for " + pid.getValue() + ")");

                    // a replica doesn't exist. add it
                    boolean updated = false;
                    if (replicable) {
                        Replica replicaMetadata = new Replica();
                        replicaMetadata.setReplicaMemberNode(targetNode.getIdentifier());
                        replicaMetadata.setReplicationStatus(ReplicationStatus.QUEUED);
                        replicaMetadata.setReplicaVerified(Calendar.getInstance().getTime());

                        try {
                            sysmeta = this.systemMetadata.get(pid); // refresh sysmeta
                                                                    // to avoid
                                                                    // VersionMismatch
                            updated = this.cnReplication.updateReplicationMetadata(
                                    session, pid, replicaMetadata, sysmeta
                                            .getSerialVersion().longValue());

                        } catch (VersionMismatch e) {

                            // retry if the serialVersion is wrong
                            try {
                                sysmeta = this.systemMetadata.get(pid);
                                updated = this.cnReplication
                                        .updateReplicationMetadata(session, pid,
                                                replicaMetadata, sysmeta
                                                        .getSerialVersion()
                                                        .longValue());

                            } catch (VersionMismatch e1) {
                                String msg = "Couldn't get the correct serialVersion to update "
                                        + "the replica metadata for identifier "
                                        + pid.getValue()
                                        + " and target node "
                                        + targetNode.getIdentifier().getValue();
                                log.info(msg);

                            } catch (BaseException be) {
                                be.printStackTrace();
                                // something is very wrong with CN self communication
                                // try the round robin address multiple times
                                updated = updateReplicationMetadata(session, pid,
                                        replicaMetadata);

                            }

                        } catch (BaseException be) {
                            be.printStackTrace();
                            // something is very wrong with CN self communication
                            // try the round robin address multiple times
                            updated = updateReplicationMetadata(session, pid,
                                    replicaMetadata);

                        } catch (RuntimeException re) {
                            re.printStackTrace();
                            log.error("Couldn't get system metadata for identifier "
                                    + pid.getValue()
                                    + " while trying to update replica "
                                    + "metadata entry for node "
                                    + replicaMetadata.getReplicaMemberNode()
                                            .getValue());

                        }

                        // create the task if the update succeeded
                        if ( updated ) {
                            log.info("Updated system metadata for identifier "
                                    + pid.getValue()
                                    + " with QUEUED replication status.");
                            log.trace("METRICS:\tREPLICATION:\tQUEUE:\tPID:\t"
                                    + pid.getValue() + "\tNODE:\t"
                                    + targetNode.getIdentifier().getValue()
                                    + "\tSIZE:\t" + sysmeta.getSize().intValue());
                            Long taskid = taskIdGenerator.newId();
                            // add the task to the task list
                            log.info("Adding a new MNReplicationTask to the queue where "
                                    + "pid = "
                                    + pid.getValue()
                                    + ", originatingNode = "
                                    + authoritativeNode.getIdentifier().getValue()
                                    + ", targetNode = "
                                    + targetNode.getIdentifier().getValue());
                            MNReplicationTask task = new MNReplicationTask(
                                    taskid.toString(), pid,
                                    authoritativeNode.getIdentifier(),
                                    targetNode.getIdentifier());
                            this.replicationTasks.add(task);
                            taskCount++;
                        }
                    }
                }
                
            } else {
                log.info("couldn't get a lock while evaluating identifier " +
                        pid.getValue() + ". Re-queuing the identifer.");
                try {
                    if ( !this.replicationEvents.contains(pid) ) {
                        this.replicationEvents.offer(pid);
                    }
                    
                } catch (Exception e) {
                    log.error("Couldn't resubmit identifier " + pid.getValue() +
                        " back onto the hzReplicationEvents queue.");
                }
                
            } // end if(isLocked)
            
        } catch (InterruptedException ie) {
            log.info("The lock was interrupted while evaluating identifier " +
                pid.getValue() + ". Re-queuing the identifer.");
            try {
                if ( !this.replicationEvents.contains(pid) ) {
                    this.replicationEvents.offer(pid);
                }
            } catch (Exception e) {
                log.error("Couldn't resubmit identifier " + pid.getValue() +
                    " back onto the hzReplicationEvents queue.");
            }
            
        } finally {
            // always unlock the identifier
            if ( isLocked ) {
                lock.unlock();
            }
        }

        // return the number of replication tasks queued
        log.info("Added " + taskCount + " MNReplicationTasks to the queue for "
                + pid.getValue());
        
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

    /*
     * Update the replica metadata against the CN router address rather than the 
     * local CN address. This only gets called if normal updates fail due to
     * local CN communication errors
     * 
     * @return true if the replica metadata are updated
     * @param session - the session
     * @param pid - the identifier of the object to be updated
     * @param replicaMetadata
     */
    private boolean updateReplicationMetadata(Session session, Identifier pid,
            Replica replicaMetadata) {
        SystemMetadata sysmeta;
        CNode cn;
        boolean updated = false;
        String baseURL = "https://" + this.cnRouterHostname + "/cn";
        cn = new CNode(baseURL);
        
        // try multiple times since at this point we may be dealing with a lame
        // CN in the cluster and the RR may still point us to it
        for (int i = 0; i < 5; i++) {
            try {
                // refresh the system metadata in case it changed
                sysmeta = this.systemMetadata.get(pid);
                updated = cn.updateReplicationMetadata(session, pid, replicaMetadata, 
                        sysmeta.getSerialVersion().longValue());
                if ( updated ) {
                    break;
                }
                
            } catch (BaseException be) {
                if ( log.isDebugEnabled() ) {
                    be.printStackTrace();
                    
                }
                log.error("Error in calling updateReplicationMetadata() " +
                    "at " + baseURL + ": " + be.getMessage());
                continue;
                
            } catch ( RuntimeException re ) {
                if ( log.isDebugEnabled() ) {
                    re.printStackTrace();
                    
                }
                log.error("Error in getting sysyem metadata from the map: " +
                    re.getMessage());
                continue;
                
            }
        }
        
        return updated;
        
    }

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
                log.debug("Executing task id " + task.getTaskid()
                        + "for identifier " + task.getPid().getValue()
                        + " and target node " + task.getTargetNode().getValue());
                try {
                    String result = task.call();
                    log.debug("Result of executing task id" + task.getTaskid()
                            + " is identifier string: " + result);

                } catch (Exception e) {
                    log.debug("Caught exception executing task id "
                            + task.getTaskid() + ": " + e.getMessage());
                    if (log.isDebugEnabled()) {
                        e.printStackTrace();

                    }

                }
                // log.info("Submitting replication task id " + task.getTaskid()
                // + " for execution with object identifier: "
                // + task.getPid().getValue() + " on replica node "
                // + task.getTargetNode().getValue());

                // TODO: handle the case when a CN drops and the
                // MNReplicationTask.call()
                // ExecutorService executorService = this.hzMember
                // .getExecutorService("ReplicationTasks");
                // Future<String> future = executorService.submit(task);

                // check for completion
                // boolean isDone = false;
                // String result = null;
                //
                // while (!isDone) {
                //
                // try {
                // result = (String) future.get(5L, TimeUnit.SECONDS);
                // log.trace("Task result for identifier "
                // + task.getPid().getValue() + " is " + result);
                // if (result != null) {
                // log.debug("Task " + task.getTaskid()
                // + " completed for identifier "
                // + task.getPid().getValue());
                //
                // }
                //
                // } catch (ExecutionException e) {
                // String msg = "";
                // if (e.getCause() != null) {
                // msg = e.getCause().getMessage();
                //
                // }
                // log.info("MNReplicationTask id "
                // + task.getTaskid()
                // + " threw an execution execption on identifier "
                // + task.getPid().getValue() + ": " + msg);
                // if (task.getRetryCount() < 10) {
                // task.setRetryCount(task.getRetryCount() + 1);
                // future.cancel(true);
                // this.replicationTasks.add(task);
                // log.info("Retrying replication task id "
                // + task.getTaskid() + " for identifier "
                // + task.getPid().getValue()
                // + " on replica node "
                // + task.getTargetNode().getValue());
                //
                // } else {
                // log.info("Replication task id"
                // + task.getTaskid()
                // + " failed, too many retries for identifier"
                // + task.getPid().getValue()
                // + " on replica node "
                // + task.getTargetNode().getValue()
                // + ". Not retrying.");
                // }
                //
                // } catch (TimeoutException e) {
                // String msg = e.getMessage();
                // log.info("Replication task id " + task.getTaskid()
                // + " timed out for identifier "
                // + task.getPid().getValue()
                // + " on replica node "
                // + task.getTargetNode().getValue() + " : " + msg);
                // future.cancel(true); // isDone() is now true
                //
                // } catch (InterruptedException e) {
                // String msg = e.getMessage();
                // log.info("Replication task id " + task.getTaskid()
                // + " was interrupted for identifier "
                // + task.getPid().getValue()
                // + " on replica node "
                // + task.getTargetNode().getValue() + " : " + msg);
                // if (task.getRetryCount() < 10) {
                // task.setRetryCount(task.getRetryCount() + 1);
                // this.replicationTasks.add(task);
                // log.info("Retrying replication task id "
                // + task.getTaskid() + " for identifier "
                // + task.getPid().getValue());
                //
                // } else {
                // log.error("Replication task id"
                // + task.getTaskid()
                // + " failed, too many retries for identifier"
                // + task.getPid().getValue()
                // + " on replica node "
                // + task.getTargetNode().getValue()
                // + ". Not retrying.");
                // }
                //
                // }
                //
                // isDone = future.isDone();
                // log.debug("Task " + task.getTaskid()
                // + " is done for identifier "
                // + task.getPid().getValue() + " on replica node "
                // + task.getTargetNode().getValue() + ": " + isDone);
                //
                // // handle canceled tasks (from the timeout period)
                // if (future.isCancelled()) {
                // log.info("Replication task id " + task.getTaskid()
                // + " was cancelled for identifier "
                // + task.getPid().getValue());
                //
                // // leave the Replica entry as QUEUED in system metadata
                // // to be picked up later
                // }
                // }

            }

        } catch (InterruptedException e) {

            String message = "Polling of the replication task queue was interrupted. "
                    + "The message was: " + e.getMessage();
            log.debug(message);
        } catch (RuntimeInterruptedException rie) {
            String message = "Hazelcast instance was lost due to cluster shutdown, "
                    + rie.getMessage();
            log.debug(message);

        } catch (IllegalStateException ise) {
            String message = "Hazelcast instance was lost due to cluster shutdown, "
                    + ise.getMessage();
            log.debug(message);
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
            log.debug("ReplicationManager.isAllowed() = " + isAllowed + " for "
                    + pid.getValue());
            isAllowed = false;

        } catch (RuntimeException re) {
            log.error("Runtime exception caught in ReplicationManager.isAllowed() "
                    + "for identifier " + pid.getValue()
                    + ". The error message was: " + re.getMessage());
            log.debug("ReplicationManager.isAllowed() = " + isAllowed + " for "
                    + pid.getValue());
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
    public Map<NodeReference, Float> getPendingRequestFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {

        return prioritizationStrategy.getPendingRequestFactors(nodeIdentifiers,
                useCache);
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
    public Map<NodeReference, Float> getFailureFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {
        return prioritizationStrategy.getFailureFactors(nodeIdentifiers, useCache);
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
    public Map<NodeReference, Float> getBandwidthFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {
        HashMap<NodeReference, Float> bandwidthFactors = new HashMap<NodeReference, Float>();

        return prioritizationStrategy.getBandwidthFactors(nodeIdentifiers, useCache);
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
    @SuppressWarnings("unchecked")
    public List<NodeReference> prioritizeNodes(
            List<NodeReference> potentialNodeList, SystemMetadata sysmeta) {

        List<NodeReference> nodesByPriority = prioritizationStrategy
                .prioritizeNodes(potentialNodeList, sysmeta);

        // if the prioritization results cause the replication policy to not
        // be fulfilled immediately (lack of currently available target nodes),
        // add the pid back onto the hzReplicationEvents queue for later
        // processing (when targets become available)
        Identifier pid = sysmeta.getIdentifier();
        log.debug("Nodes by priority list size: " + nodesByPriority.size());
        int desiredCount = sysmeta.getReplicationPolicy().getNumberReplicas();
        if (nodesByPriority.size() >= desiredCount) {
            log.debug("There are enough target nodes to fulfill the replication "
                    + "policy. Not resubmitting identifier " + pid.getValue());
        } else {
            log.debug("There are not enough target nodes to fulfill the replication "
                    + "policy. Resubmitting identifier " + pid.getValue());
            if (!this.replicationEvents.contains(pid)) {
                boolean resubmitted = this.replicationEvents.offer(pid);
                if (resubmitted) {
                    log.debug("Successfully resubmitted identifier "
                            + pid.getValue());

                } else {
                    log.warn("Couldn't resubmit identifier " + pid.getValue());

                }

            } else {
                log.debug("Identifier " + pid.getValue() + " is already in "
                        + "the hzReplicationEvents queue, not resubmitting.");

            }

        }
        return nodesByPriority;
    }
}
