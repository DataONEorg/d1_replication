/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
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
 * 
 */

package org.dataone.service.cn.replication.v1;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.data.repository.ReplicationAttemptHistory;
import org.dataone.cn.data.repository.ReplicationAttemptHistoryRepository;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.hazelcast.HazelcastInstanceFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
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
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;

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
public class ReplicationManager {

    /* Get a Log instance */
    public static Logger log = Logger.getLogger(ReplicationManager.class);

    private ReplicationService replicationService;

    /* The instance of the Hazelcast storage cluster client */
    private HazelcastClient hzClient;

    /* the instance of the Hazelcast processing cluster member */
    private HazelcastInstance hzMember;

    /* The name of the node map */
    private String nodeMap;

    /* The name of the system metadata map */
    private String systemMetadataMap;

    /* The Hazelcast distributed system metadata map */
    private IMap<NodeReference, Node> nodes;

    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadata;

    private ReplicationTaskQueue replicationTaskQueue;

    /* The Hazelcast distributed counts by node-status map name */
    private String nodeReplicationStatusMap;

    /* The Hazelcast distributed map of status counts by node-status */
    private IMap<String, Integer> nodeReplicationStatus;

    /* A client reference to the coordinating node */
    private CNReplication cnReplication = null;

    /* The prioritization strategy for determining target replica nodes */
    private ReplicationPrioritizationStrategy prioritizationStrategy = new ReplicationPrioritizationStrategy();

    private static ScheduledExecutorService staleRequestedReplicaAuditScheduler;
    private static ScheduledExecutorService staleQueuedReplicaAuditScheduler;

    private static ScheduledExecutorService replicationTaskProcessingScheduler;

    private ReplicationAttemptHistoryRepository replicationAttemptHistoryRepository;
    private ReplicationTaskRepository taskRepository;

    private static final Integer REPLICATION_ATTEMPTS_PER_DAY = Integer.valueOf(10);

    /* The router hostname for the CN cluster (round robin) */
    private String cnRouterHostname;

    /**
     * Constructor - singleton pattern, enforced by Spring application. Although
     * there is only one instance of ReplicationManager per JVM, the CNs run in
     * seperate JVMs and therefore 3 or more instances of ReplicationManager may
     * be operating on the same events and data
     */
    public ReplicationManager() {

        this.nodeMap = Settings.getConfiguration().getString("dataone.hazelcast.nodes");
        this.systemMetadataMap = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");
        this.nodeReplicationStatusMap = Settings.getConfiguration().getString(
                "dataone.hazelcast.nodeReplicationStatusMap");

        this.cnRouterHostname = Settings.getConfiguration().getString("cn.router.hostname");

        // Connect to the Hazelcast storage cluster
        this.hzClient = HazelcastClientFactory.getStorageClient();

        // Connect to the Hazelcast process cluster
        log.info("Becoming a DataONE Process cluster hazelcast member with the default instance.");
        this.hzMember = HazelcastInstanceFactory.getProcessingInstance();

        // get references to cluster structures
        this.nodes = this.hzMember.getMap(this.nodeMap);
        this.systemMetadata = this.hzClient.getMap(this.systemMetadataMap);
        this.nodeReplicationStatus = this.hzMember.getMap(this.nodeReplicationStatusMap);

        this.replicationTaskQueue = ReplicationFactory.getReplicationTaskQueue();
        this.replicationService = ReplicationFactory.getReplicationService();

        this.replicationAttemptHistoryRepository = ReplicationFactory
                .getReplicationTryHistoryRepository();
        this.taskRepository = ReplicationFactory.getReplicationTaskRepository();

        startStaleRequestedAuditing();
        startStaleQueuedAuditing();

        startReplicationTaskProcessing();

        // Report node status statistics on a scheduled basis
        // TODO: hold off on scheduling code for now
        // reportScheduler = Executors.newSingleThreadScheduledExecutor();
        // this.reportCountsTask =
        // reportScheduler.scheduleAtFixedRate(new Runnable(){
        //
        // @Override
        // public void run() {
        // // TODO Auto-generated method stub
        //
        // }
        //
        // }, 0L, 1L, TimeUnit.SECONDS);

        // Set up the certificate location, create a null session
        String clientCertificateLocation = Settings.getConfiguration().getString(
                "D1Client.certificate.directory")
                + File.separator
                + Settings.getConfiguration().getString("D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        log.info("ReplicationManager is using an X509 certificate from "
                + clientCertificateLocation);
        init();
    }

    private void init() {
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
                    log.error("There was a problem getting a Coordinating Node reference.", e1);
                }
                cnode = D1Client.getCN();

            } catch (ServiceFailure e1) {
                log.error("There was a problem getting a Coordinating Node reference "
                        + " for the ReplicationManager. ", e1);
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
     */
    public int createAndQueueTasks(Identifier pid) {

        log.debug("ReplicationManager.createAndQueueTasks called.");

        //replicationTaskQueue.logState();

        ILock lock;
        boolean allowed;
        int taskCount = 0;
        int desiredReplicas = 3;
        long timeToWait = 5L;
        List<NodeReference> listedReplicaNodes = new ArrayList<NodeReference>();
        Set<NodeReference> nodeList; // the full nodes list
        List<NodeReference> potentialNodeList; // the MN subset of the nodes
                                               // list
        Node targetNode = new Node(); // the target node for the replica
        Node authoritativeNode = new Node(); // the source node of the object
        SystemMetadata sysmeta;

        // use the distributed lock
        lock = null;
        String lockPid = pid.getValue();
        lock = this.hzMember.getLock(lockPid);
        boolean isLocked = false;
        try {
            isLocked = lock.tryLock(timeToWait, TimeUnit.MILLISECONDS);
            // only evaluate an identifier if we can get the lock fairly
            // instantly
            if (isLocked) {
                // if replication isn't allowed, return
                allowed = isAllowed(pid);
                log.debug("Replication is allowed for identifier " + pid.getValue());
                if (!allowed) {
                    log.debug("Replication is not allowed for the object identified by "
                            + pid.getValue());
                    return 0;

                }
                // get the system metadata for the pid
                log.debug("Getting the replica list for identifier " + pid.getValue());
                sysmeta = this.systemMetadata.get(pid);

                // add already listed replicas to listedReplicaNodes
                listedReplicaNodes = getCurrentReplicaList(sysmeta);
                int currentListedReplicaCount = listedReplicaNodes.size();

                // parse the sysmeta.ReplicationPolicy
                ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();

                // set the desired replicas if present
                if (replicationPolicy.getNumberReplicas() != null) {
                    desiredReplicas = replicationPolicy.getNumberReplicas().intValue();
                }

                int desiredReplicasLessListed = desiredReplicas - currentListedReplicaCount;
                log.debug("Desired replica count less already listed replica count is "
                        + desiredReplicasLessListed);

                // List of Nodes for building MNReplicationTasks
                log.debug("Building a potential target node list for identifier " + pid.getValue());
                nodeList = (Set<NodeReference>) this.nodes.keySet();
                potentialNodeList = new ArrayList<NodeReference>(); // will be
                                                                    // our short
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
                    // only add MNs as targets, excluding the authoritative MN
                    // and MNs that are not tagged to replicate
                    if ((node.getType() == NodeType.MN)
                            && node.isReplicate()
                            && !node.getIdentifier().getValue()
                                    .equals(authoritativeNode.getIdentifier().getValue())
                            && isUnderReplicationAttemptsPerDay(pid, nodeReference)) {
                        potentialNodeList.add(node.getIdentifier());
                    }
                }

                // then remove the already listed replica nodes
                potentialNodeList.removeAll(listedReplicaNodes);

                // prioritize replica targets by preferred/blocked lists and
                // other
                // performance metrics
                log.trace("METRICS:\tPRIORITIZE:\tPID:\t" + pid.getValue());
                potentialNodeList = prioritizeNodes(desiredReplicasLessListed, potentialNodeList,
                        sysmeta);
                log.trace("METRICS:\tEND PRIORITIZE:\tPID:\t" + pid.getValue());

                // report node-status counts to hazelcast.
                // TODO: This may be removed in favor of scheduled reporting.
                reportCountsByNodeStatus();

                if (!potentialNodeList.isEmpty()) {
                    log.info("Desired replicas for identifier " + pid.getValue() + " is "
                            + desiredReplicasLessListed);
                    log.info("Potential target node list size for " + pid.getValue() + " is "
                            + potentialNodeList.size());
                    // can't have more replicas than MNs
                    if (desiredReplicasLessListed > potentialNodeList.size()) {
                        desiredReplicasLessListed = potentialNodeList.size(); // yikes
                        log.info("Changed the desired replicas for identifier " + pid.getValue()
                                + " to the size of the potential target node list: "
                                + potentialNodeList.size());
                    }

                    // reset desiredReplicasLessListed to avoid task creation
                    // in the ' 0 > any negative nuber' scenario
                    if (desiredReplicasLessListed < 0) {
                        desiredReplicasLessListed = 0;
                    }

                    // for each node in the potential node list up to the
                    // desired replicas
                    // (less the pending/completed replicas)
                    for (int j = 0; j < desiredReplicasLessListed; j++) {

                        log.debug("Evaluating item " + j + " of " + desiredReplicasLessListed
                                + " in the potential node list.");
                        NodeReference potentialNode = potentialNodeList.get(j);

                        targetNode = this.nodes.get(potentialNode);
                        log.debug("currently evaluating " + targetNode.getIdentifier().getValue()
                                + " for task creation " + "for identifier " + pid.getValue());

                        // may be more than one version of MNReplication
                        List<String> implementedVersions = new ArrayList<String>();
                        List<Service> origServices = authoritativeNode.getServices()
                                .getServiceList();
                        for (Service service : origServices) {
                            if (service.getName().equals("MNReplication") && service.getAvailable()) {
                                implementedVersions.add(service.getVersion());
                            }
                        }
                        if (implementedVersions.isEmpty()) {
                            continue; // this node is not replicable
                        }

                        boolean replicable = false;

                        for (Service service : targetNode.getServices().getServiceList()) {
                            if (service.getName().equals("MNReplication")
                                    && implementedVersions.contains(service.getVersion())
                                    && service.getAvailable()) {
                                replicable = true;
                            }
                        }
                        log.debug("Based on evaluating the target node services, node id "
                                + targetNode.getIdentifier().getValue() + " is replicable: "
                                + replicable + " (during evaluation for " + pid.getValue() + ")");

                        // a replica doesn't exist. add it
                        boolean updated = false;
                        if (replicable) {
                            Replica replicaMetadata = new Replica();
                            replicaMetadata.setReplicaMemberNode(targetNode.getIdentifier());
                            replicaMetadata.setReplicationStatus(ReplicationStatus.QUEUED);
                            replicaMetadata.setReplicaVerified(Calendar.getInstance().getTime());

                            try {
                                // refresh sysmeta to avoid VersionMismatch
                                sysmeta = this.systemMetadata.get(pid);
                                updated = this.cnReplication.updateReplicationMetadata(pid,
                                        replicaMetadata, sysmeta.getSerialVersion().longValue());

                            } catch (VersionMismatch e) {

                                // retry if the serialVersion is wrong
                                try {
                                    sysmeta = this.systemMetadata.get(pid);
                                    updated = this.cnReplication
                                            .updateReplicationMetadata(pid, replicaMetadata,
                                                    sysmeta.getSerialVersion().longValue());

                                } catch (VersionMismatch e1) {
                                    String msg = "Couldn't get the correct serialVersion to update "
                                            + "the replica metadata for identifier "
                                            + pid.getValue()
                                            + " and target node "
                                            + targetNode.getIdentifier().getValue();
                                    log.error(msg);

                                } catch (BaseException be) {
                                    // the replica has already completed from a
                                    // different task
                                    if (be instanceof InvalidRequest) {
                                        log.warn(
                                                "Couldn't update replication metadata to "
                                                        + replicaMetadata.getReplicationStatus()
                                                                .toString()
                                                        + ", it may have possibly already been updated for identifier "
                                                        + pid.getValue()
                                                        + " and target node "
                                                        + replicaMetadata.getReplicaMemberNode()
                                                                .getValue() + ". The error was: "
                                                        + be.getMessage(), be);
                                    } else {
                                        if (log.isDebugEnabled()) {
                                            log.debug(be);
                                        }
                                        // something is very wrong with CN self
                                        // communication
                                        // try the round robin address multiple
                                        // times
                                        updated = replicationService.updateReplicationMetadata(pid,
                                                replicaMetadata);

                                    }

                                }

                            } catch (BaseException be) {
                                // the replica has already completed from a
                                // different task
                                if (be instanceof InvalidRequest) {
                                    log.warn(
                                            "Couldn't update replication metadata to "
                                                    + replicaMetadata.getReplicationStatus()
                                                            .toString()
                                                    + ", it may have possibly already been updated for identifier "
                                                    + pid.getValue()
                                                    + " and target node "
                                                    + replicaMetadata.getReplicaMemberNode()
                                                            .getValue() + ". The error was: "
                                                    + be.getMessage(), be);
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug(be);

                                    }
                                    // something is very wrong with CN self
                                    // communication
                                    // try the round robin address multiple
                                    // times
                                    updated = replicationService.updateReplicationMetadata(pid,
                                            replicaMetadata);

                                }

                            } catch (RuntimeException re) {
                                log.error(
                                        "Couldn't get system metadata for identifier "
                                                + pid.getValue()
                                                + " while trying to update replica "
                                                + "metadata entry for node "
                                                + replicaMetadata.getReplicaMemberNode().getValue(),
                                        re);

                            }
                            if (updated) {
                                taskCount++;
                                this.replicationTaskQueue.processAllTasksForMN(targetNode
                                        .getIdentifier().getValue());
                            } else {
                                log.error("CN.updateReplicationMetadata() failed for "
                                        + "identifier " + pid.getValue() + ", node "
                                        + targetNode.getIdentifier().getValue()
                                        + ". Task not created.");
                            }
                        }
                    } // end for()
                } // end if potentialNodeList is NOT empty
            } else {
                log.info("Couldn't get a lock while evaluating identifier " + pid.getValue()
                        + ". Assuming another CN handled it.");
                // try {
                // if ( !this.replicationEvents.contains(pid) ) {
                // boolean taken = this.replicationEvents.offer(pid);
                // if (taken == false) {
                // log.error("ReplicationEvents.offer() returned false for pid: "
                // + pid);
                // }
                // }
                //
                // } catch (Exception e) {
                // log.error("Couldn't resubmit identifier " + pid.getValue() +
                // " back onto the hzReplicationEvents queue.");
                // }

            } // end if(isLocked)

        } catch (InterruptedException ie) {
            log.info("The lock was interrupted while evaluating identifier " + pid.getValue()
                    + ". Re-queuing the identifer.");
            requeueReplicationTask(pid);
        } catch (Exception e) {
            log.error(
                    "Unhandled Exception for pid: " + pid.getValue() + ". Error is : "
                            + e.getMessage(), e);
        } finally {
            // always unlock the identifier
            if (isLocked) {
                lock.unlock();
            }
        }

        // return the number of replication tasks queued
        log.info("Added " + taskCount + " MNReplicationTasks to the queue for " + pid.getValue());

        return taskCount;

    }

    private void requeueReplicationTask(Identifier pid) {
        List<ReplicationTask> taskList = taskRepository.findByPid(pid.getValue());
        if (taskList.size() == 1) {
            ReplicationTask task = taskList.get(0);
            task.markNew();
            taskRepository.save(task);
        } else if (taskList.size() == 0) {
            log.warn("In Replication Manager, task that should exist 'in process' does not exist.  Creating new task for pid: "
                    + pid.getValue());
            taskRepository.save(new ReplicationTask(pid));
        } else if (taskList.size() > 1) {
            log.warn("In Replication Manager, more than one task found for pid: " + pid.getValue()
                    + ". Deleting all and creating new task.");
            taskRepository.delete(taskList);
            taskRepository.save(new ReplicationTask(pid));
        }
    }

    /**
     * Check if replication is allowed for the given pid
     * 
     * @param pid
     *            - the identifier of the object to check
     * @return
     */
    private boolean isAllowed(Identifier pid) {

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

    /**
     * Check to see if replication tasks are pending for the given pid
     * 
     * @param pid
     *            - the identifier of the object to check
     */
    private boolean isPending(Identifier pid) {
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

    /**
     * Given the current system metadata object, return a list of already listed
     * replica entries that are in the queued, requested, or completed state.
     * 
     * @param sysmeta
     *            the system metadata object to evaluate
     * @return listedReplicaNodes the list of currently listed replica nodes
     */
    public List<NodeReference> getCurrentReplicaList(SystemMetadata sysmeta) {
        List<NodeReference> listedReplicaNodes = new ArrayList<NodeReference>();
        List<Replica> replicaList; // the replica list for this pid
        replicaList = sysmeta.getReplicaList();
        if (replicaList == null) {
            replicaList = new ArrayList<Replica>();

        }
        for (Replica listedReplica : replicaList) {
            NodeReference nodeId = listedReplica.getReplicaMemberNode();
            ReplicationStatus listedStatus = listedReplica.getReplicationStatus();

            Node node = new Node();
            NodeType nodeType = null;
            try {
                node = this.nodes.get(nodeId);
                if (node != null) {
                    log.debug("The potential target node id is: " + node.getIdentifier().getValue());
                    nodeType = node.getType();
                    if (nodeType == NodeType.CN
                            || nodeId.getValue().equals(
                                    sysmeta.getAuthoritativeMemberNode().getValue())) {
                        continue; // don't count CNs or authoritative nodes as
                                  // replicas
                    }
                } else {
                    log.debug("The potential target node id is null for " + nodeId.getValue());
                    continue;
                }
            } catch (Exception e) {
                log.debug("There was an error getting the node type: " + e.getMessage(), e);
            }
            if (listedStatus == ReplicationStatus.QUEUED
                    || listedStatus == ReplicationStatus.REQUESTED
                    || listedStatus == ReplicationStatus.COMPLETED
                    || listedStatus == ReplicationStatus.INVALIDATED) {
                listedReplicaNodes.add(nodeId);
            }
        }
        return listedReplicaNodes;
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
    private Map<NodeReference, Float> getPendingRequestFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {

        return prioritizationStrategy.getPendingRequestFactors(nodeIdentifiers, useCache);
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
    private Map<NodeReference, Float> getFailureFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {
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
    private Map<NodeReference, Float> getBandwidthFactors(List<NodeReference> nodeIdentifiers,
            boolean useCache) {
        HashMap<NodeReference, Float> bandwidthFactors = new HashMap<NodeReference, Float>();

        return prioritizationStrategy.getBandwidthFactors(nodeIdentifiers, useCache);
    }

    /**
     * Report replica counts by node status by querying the system metadata
     * replication status tables on the CNs and push the results into a deicated
     * hazelcast map
     **/
    private void reportCountsByNodeStatus() {

        /* A map used to transfer records from the DAO query into hazelcast */
        Map<String, Integer> countsByNodeStatusMap = new HashMap<String, Integer>();
        try {
            countsByNodeStatusMap = DaoFactory.getReplicationDao().getCountsByNodeStatus();
            log.debug("Counts by Node-Status map size: " + countsByNodeStatusMap.size());
        } catch (DataAccessException e) {
            log.info("There was an error getting node-status counts: " + e.getMessage());
            if (log.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
        this.nodeReplicationStatus.putAll(countsByNodeStatusMap);
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
    private List<NodeReference> prioritizeNodes(int desiredReplicasLessListed,
            List<NodeReference> potentialNodeList, SystemMetadata sysmeta) {

        List<NodeReference> nodesByPriority = prioritizationStrategy.prioritizeNodes(
                potentialNodeList, sysmeta);

        // if the prioritization results cause the replication policy to not
        // be fulfilled immediately (lack of currently available target nodes),
        // add the pid back onto the hzReplicationEvents queue for later
        // processing (when targets become available)
        Identifier pid = sysmeta.getIdentifier();
        log.debug("Nodes by priority list size: " + nodesByPriority.size());
        if (nodesByPriority.size() >= desiredReplicasLessListed) {
            log.debug("There are enough target nodes to fulfill the replication "
                    + "policy. Not resubmitting identifier " + pid.getValue());
        } else {
            log.debug("There are not enough target nodes to fulfill the replication "
                    + "policy. Resubmitting identifier " + pid.getValue());
            requeueReplicationTask(pid);
        }
        return nodesByPriority;
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

    private void startReplicationTaskProcessing() {
        if (replicationTaskProcessingScheduler == null
                || replicationTaskProcessingScheduler.isShutdown()) {
            replicationTaskProcessingScheduler = Executors.newSingleThreadScheduledExecutor();
            replicationTaskProcessingScheduler.scheduleAtFixedRate(new ReplicationTaskProcessor(),
                    0L, 2L, TimeUnit.MINUTES);
        }
    }

    private boolean isUnderReplicationAttemptsPerDay(Identifier identifier,
            NodeReference nodeReference) {

        if (identifier == null || identifier.getValue() == null || nodeReference == null
                || nodeReference.getValue() == null) {
            return false;
        }

        boolean underAttemptsPerDay = false;

        ReplicationAttemptHistory attemptHistory = findReplicationAttemptHistoryRecord(identifier,
                nodeReference);

        // no attempt history yet, create one
        if (attemptHistory == null) {
            attemptHistory = new ReplicationAttemptHistory(identifier, nodeReference,
                    Integer.valueOf(1));
            attemptHistory = replicationAttemptHistoryRepository.save(attemptHistory);
            underAttemptsPerDay = true;

        } else if (attemptHistory != null) {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.HOUR, -24);

            // if last attempt date was more than 24 hours ago:
            if (attemptHistory.getLastReplicationAttemptDate() < calendar.getTimeInMillis()) {
                // reset count to 1 (this attempt)
                attemptHistory.setReplicationAttempts(Integer.valueOf(1));
                // set last attempt date to now.
                attemptHistory.setLastReplicationAttemptDate(System.currentTimeMillis());
                attemptHistory = replicationAttemptHistoryRepository.save(attemptHistory);
                // set result to true
                underAttemptsPerDay = true;

                // if last attempt less than 24 hours ago:
            } else if (attemptHistory.getLastReplicationAttemptDate() >= calendar.getTimeInMillis()) {
                // if count is less than max attempts:
                if (attemptHistory.getReplicationAttempts().intValue() < REPLICATION_ATTEMPTS_PER_DAY) {
                    // increment count by 1
                    attemptHistory.incrementReplicationAttempts();
                    // set last attempt date to now.
                    attemptHistory.setLastReplicationAttemptDate(System.currentTimeMillis());
                    attemptHistory = replicationAttemptHistoryRepository.save(attemptHistory);
                    // set result to true
                    underAttemptsPerDay = true;

                    // if count is equal or greater than max
                } else if (attemptHistory.getReplicationAttempts() >= REPLICATION_ATTEMPTS_PER_DAY) {
                    // do not increment last attempt date or count
                    // set result to false
                    underAttemptsPerDay = false;
                }
            }
        }
        return underAttemptsPerDay;
    }

    private ReplicationAttemptHistory findReplicationAttemptHistoryRecord(Identifier identifier,
            NodeReference nodeReference) {

        List<ReplicationAttemptHistory> attemptHistoryList = replicationAttemptHistoryRepository
                .findByPidAndNodeId(identifier.getValue(), nodeReference.getValue());

        ReplicationAttemptHistory attemptHistory = null;

        if (attemptHistoryList.size() > 1) {
            log.error("More than one replication attempt history exists for pid: "
                    + identifier.getValue() + " and node: " + nodeReference.getValue()
                    + ".  Using first result, deleting others.");
            for (ReplicationAttemptHistory attemptHistoryResult : attemptHistoryList) {
                if (attemptHistory == null) {
                    attemptHistory = attemptHistoryResult;
                } else {
                    replicationAttemptHistoryRepository.delete(attemptHistoryResult);
                }
            }
        } else if (attemptHistoryList.size() == 1) {
            attemptHistory = attemptHistoryList.get(0);
        }

        return attemptHistory;
    }
}
