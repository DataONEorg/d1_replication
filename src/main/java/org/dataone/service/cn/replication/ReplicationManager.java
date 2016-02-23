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

package org.dataone.service.cn.replication;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.dataone.client.D1NodeFactory;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.data.repository.ReplicationAttemptHistory;
import org.dataone.cn.data.repository.ReplicationAttemptHistoryRepository;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.impl.v2.NodeRegistryService;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeReplicationPolicy;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
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

    private HazelcastClient hzProcessingClient;

    private NodeRegistryService nodeService = new NodeRegistryService();

    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadataMap;

    private ReplicationTaskQueue replicationTaskQueue;

    /* The Hazelcast distributed counts by node-status map name */
    private String nodeReplicationStatusMapName;

    /* The Hazelcast distributed map of status counts by node-status */
    private IMap<String, Integer> nodeReplicationStatusMap;

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

    /**
     * Constructor - singleton pattern, enforced by Spring application. Although
     * there is only one instance of ReplicationManager per JVM, the CNs run in
     * seperate JVMs and therefore 3 or more instances of ReplicationManager may
     * be operating on the same events and data
     */
    public ReplicationManager() {
        this(null);
    }

    /**
     * A for-testing-purposes constructor to get around configuration complications
     * @param repAttemptHistoryRepos
     */
    public ReplicationManager(ReplicationRepositoryFactory repositoryFactory) {

        this.nodeReplicationStatusMapName = Settings.getConfiguration().getString(
                "dataone.hazelcast.nodeReplicationStatusMap");

        // Connect to the Hazelcast storage cluster
        this.hzClient = HazelcastClientFactory.getStorageClient();

        // Connect to the Hazelcast process cluster
        this.hzProcessingClient = HazelcastClientFactory.getProcessingClient();

        // get references to cluster structures
        this.systemMetadataMap = HazelcastClientFactory.getSystemMetadataMap();
        this.nodeReplicationStatusMap = this.hzProcessingClient.getMap(this.nodeReplicationStatusMapName);

        this.replicationTaskQueue = ReplicationFactory.getReplicationTaskQueue();
        this.replicationService = ReplicationFactory.getReplicationService();

        if (repositoryFactory != null) {
            this.taskRepository = repositoryFactory.getReplicationTaskRepository();
            this.replicationAttemptHistoryRepository = repositoryFactory
                    .getReplicationTryHistoryRepository();
        } else {
            this.replicationAttemptHistoryRepository = ReplicationFactory
                    .getReplicationTryHistoryRepository();
            this.taskRepository = ReplicationFactory.getReplicationTaskRepository();

        }

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

    /*
     * Initialize the environment context using D1Client.getCN(), and set the
     * cnReplication property of this instance
     */
    private void init() {
        log.info("initialization");
        CNode cnode = null;
        String settingsBaseUrl = Settings.getConfiguration().getString("D1Client.CN_URL");
        log.debug("D1Client.CN_URL = " + settingsBaseUrl);

        // Get an CNode reference to communicate with
        try {
            cnode = D1Client.getCN();
            log.info("ReplicationManager D1Client base_url is: " + cnode.getNodeBaseServiceUrl());
        } catch (BaseException e) {
            try {
                try {
                    Thread.sleep(5000L);

                } catch (InterruptedException e1) {
                    log.error("There was a problem getting a Coordinating Node reference.", e1);
                }
                cnode = D1Client.getCN();

            } catch (BaseException e1) {
                log.warn("Building CNode without baseURL check.");
                try {
                    cnode = D1NodeFactory.buildNode(CNode.class, new DefaultHttpMultipartRestClient(),
                            URI.create(settingsBaseUrl));
                } catch (Exception e2) {
                    log.error("There was a problem getting a Coordinating Node reference "
                            + " for the ReplicationManager. ", e2);
                    throw new RuntimeException(e2);
                }
            }
        }
        this.cnReplication = cnode;
    }

    /**
     * Get a Node from the NodeRegistryService
     * @param nodeRef
     * @return
     */
    public Node getNode(NodeReference nodeRef) {
        Node node = null;
        try {
            node = this.nodeService.getNode(nodeRef);
        } catch (ServiceFailure e) {
            log.error(
                    "Unable to locate node from node service for node ref: " + nodeRef.getValue(),
                    e);
            e.printStackTrace();
        } catch (NotFound e) {
            log.error(
                    "Unable to locate node from node service for node ref: " + nodeRef.getValue(),
                    e);
            e.printStackTrace();
        }
        return node;
    }

    /** 
     * Get a list of NodeReferences from the NodeRegistryService
     * @return
     */
    public Set<NodeReference> getNodeReferences() {
        Set<NodeReference> nodeRefs = new HashSet<NodeReference>();
        try {
            for (Node node : nodeService.listNodes().getNodeList()) {
                NodeReference nodeRef = new NodeReference();
                nodeRef.setValue(node.getIdentifier().getValue());
                nodeRefs.add(nodeRef);
            }
        } catch (NotImplemented ni) {
            log.error("Unable to get node list from node registry service", ni);
            ni.printStackTrace();
        } catch (ServiceFailure sf) {
            log.error("Unable to get node list from node registry service", sf);
            sf.printStackTrace();
        }
        return nodeRefs;
    }

    /**
     * returns list of versions for registered MNReplication services
     * @param sysmeta
     * @return
     */
    protected List<String> getObjectVersion(SystemMetadata sysmeta) {

        List<String> versions = new LinkedList<String>();
        Node n = getNode(sysmeta.getAuthoritativeMemberNode());
        if (n != null && n.getServices() != null && n.getServices().getServiceList() != null) {
            for (Service s : n.getServices().getServiceList()) {
                if (s.getName().equals("MNReplication")) {
                    versions.add(s.getVersion());
                }
            }
        }
        return versions;
    }

    /**
     * uses the system metadata and cn NodeList to determine the set of target
     * nodes available as replication targets for that object.  
     * @param sysmeta
     * @return
     */
    private Set<NodeReference> determinePossibleReplicationTargets(final SystemMetadata sysmeta) {

        Set<NodeReference> possibleReplicationTargets = new HashSet<NodeReference>();

        if (sysmeta.getReplicationPolicy() != null) {
            try {
                if (sysmeta.getReplicationPolicy().getReplicationAllowed()) {
                    for (Node node : nodeService.listNodes().getNodeList()) {
                        if (isPossibleReplicationTarget(node, sysmeta)) {
                            NodeReference nodeRef = new NodeReference();
                            nodeRef.setValue(node.getIdentifier().getValue());
                            possibleReplicationTargets.add(nodeRef);
                        }
                    }

                }
            } catch (NotImplemented ni) {
                log.error("Unable to get node list from node registry service", ni);
                ni.printStackTrace();
            } catch (ServiceFailure sf) {
                log.error("Unable to get node list from node registry service", sf);
                sf.printStackTrace();
            }

        }
        return possibleReplicationTargets;
    }

    public boolean isPossibleReplicationTarget(Node node, SystemMetadata sysmeta) {
        if (node.getType() != NodeType.MN)
            return false;

        if (!node.isReplicate())
            return false;

        if (sysmeta.getAuthoritativeMemberNode().equals(node.getIdentifier()))
            return false;

        if (getCurrentReplicaList(sysmeta).contains(node.getIdentifier()))
            return false;

        if (sysmeta.getReplicationPolicy() != null
                && sysmeta.getReplicationPolicy().getBlockedMemberNodeList()
                        .contains(node.getIdentifier()))
            return false;

        // if the target node doesn't have a policy, there are no more criteria to meet
        if (node.getNodeReplicationPolicy() == null)
            return true;
        
        NodeReplicationPolicy nrp = node.getNodeReplicationPolicy();
        if (nrp.getMaxObjectSize() != null && nrp.getMaxObjectSize().compareTo(sysmeta.getSize()) < 0)
            return false;

        List<ObjectFormatIdentifier> allowedFormats = nrp.getAllowedObjectFormatList();
        if (allowedFormats != null && !allowedFormats.contains(sysmeta.getFormatId()))
            return false;

        List<NodeReference> allowedNodes = nrp.getAllowedNodeList();
        if (allowedNodes != null && !allowedNodes.contains(sysmeta.getAuthoritativeMemberNode()))
            return false;

        return true;
    }

    /**
     * Create replication tasks given the identifier of an object by evaluating
     * its system metadata and the capabilities of the target replication nodes.
     * Queue the tasks for processing.
     * 
     * Current logic uses the following truth table to determine target MN for
     * replication:
     *  
     * Replication source/target truth table:
     * Source    Target    Ok to Replicate
     *  v1        v1          yes (v1)
     *  v1        v2          no 
     *  v1        v1,v2       yes (v1)
     *  v2        v1          no        
     *  v2        v2          yes (v2)
     *  v2        v1,v2       yes (v2)
     *  v1,v2     v1          no
     *  v1,v2     v2          yes (v2)
     *  v1,v2     v1,v2       yes (v2)
     * 
     * @param pid
     *            - the identifier of the object to be replicated
     * @return count - the number of replication tasks queued
     */
    public int createAndQueueTasks(Identifier pid) {

        log.debug("ReplicationManager.createAndQueueTasks called.");

        //replicationTaskQueue.logState();

        
        int taskCount = 0;

        long timeToWait = 5L;

        // use the distributed lock
        String lockPid = pid.getValue();
        ILock lock = this.hzProcessingClient.getLock(lockPid);
        boolean isLocked = false;
        try {
            isLocked = lock.tryLock(timeToWait, TimeUnit.MILLISECONDS);
            // only evaluate an identifier if we can get the lock fairly
            // instantly
            if (isLocked) {
                
                taskCount = processPid(pid);
                
            } else {
                log.info("Couldn't get a lock while evaluating identifier " + pid.getValue()
                        + ". Assuming another CN handled it.");
            } // end if(isLocked)

        } catch (InterruptedException ie) {
            requeueReplicationTask(pid);
            log.info("The lock was interrupted while evaluating identifier " + pid.getValue()
                    + ". Re-queuing the identifer.");
        } catch (Exception e) {
            requeueReplicationTask(pid);
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
        if (taskCount == 0) {
            requeueReplicationTask(pid);
        }

        return taskCount;
    }
    
    /**
     * helper method to createAndQueueTasks that does all the work.
     * @param pid
     * @return
     * @throws InvalidRequest
     * @throws ServiceFailure
     * @throws NotFound
     */
    private int processPid(Identifier pid) throws InvalidRequest, ServiceFailure, NotFound {
        
        // if replication isn't allowed, clean-up and return
        boolean allowed = isAllowed(pid);
        if (!allowed) {
            log.debug("Replication is not allowed for the object identified by "
                    + pid.getValue());
            removeReplicationTasksForPid(pid);
            return 0;
        }
        log.debug("Replication is allowed for identifier " + pid.getValue());


        int desiredReplicas = 3;
        int taskCount = 0;
        
        // get the system metadata for the pid
        log.debug("Getting the replica list for identifier " + pid.getValue());
        SystemMetadata sysmeta = this.systemMetadataMap.get(pid);

        // add already listed replicas to listedReplicaNodes
        List<NodeReference> listedReplicaNodes = getCurrentReplicaList(sysmeta);
        int currentListedReplicaCount = listedReplicaNodes.size();

        // set the desired replicas if present
        try {
            desiredReplicas = sysmeta.getReplicationPolicy().getNumberReplicas().intValue();
        } catch (NullPointerException npe) {
            // defaults to starting value
        }

        if (desiredReplicas <= currentListedReplicaCount) {
            log.debug("Have enough replicas already");
            removeReplicationTasksForPid(pid);
            return 0;
        }
        
        int neededReplicas = desiredReplicas - currentListedReplicaCount;
        log.debug("Needed replicas " + neededReplicas);



        // List of Nodes for building MNReplicationTasks
        log.debug("Building a potential target node list for identifier " + pid.getValue());

        Node authoritativeNode = nodeService.getNode(sysmeta.getAuthoritativeMemberNode());
        List<ApiVersion> sourceReplicationSupport = getSupportedReplicationVersions(pid, authoritativeNode);


        List<NodeReference> potentialTargetNodes = new ArrayList<>(); 
     
        if (sourceReplicationSupport.size() > 0) {

            // build the potential list of target nodes
            Set<NodeReference> nodeList = getNodeReferences();
            potentialTargetNodes = getPotentialTargetNodes(nodeList, sysmeta, sourceReplicationSupport);

            // then remove the already listed replica nodes
            potentialTargetNodes.removeAll(listedReplicaNodes);

            // prioritize replica targets by preferred/blocked lists and
            // other
            // performance metrics
            log.trace("METRICS:\tPRIORITIZE:\tPID:\t" + pid.getValue());
            potentialTargetNodes = prioritizeNodes(neededReplicas,
                    potentialTargetNodes, sysmeta);
            log.trace("METRICS:\tEND PRIORITIZE:\tPID:\t" + pid.getValue());
        }

        reportCountsByNodeStatus();

        if (!potentialTargetNodes.isEmpty()) {
            log.info("Number of replicas desired for identifier " + pid.getValue() + " is "
                    + neededReplicas);
            log.info("Potential target node list size for " + pid.getValue() + " is "
                    + potentialTargetNodes.size());
            // can't have more replicas than MNs
            if (neededReplicas > potentialTargetNodes.size()) {
                neededReplicas = potentialTargetNodes.size(); // yikes
                log.info("Changed the desired replicas for identifier " + pid.getValue()
                        + " to the size of the potential target node list: "
                        + potentialTargetNodes.size());
            }
            
            taskCount = createAndQueueTasks(pid, potentialTargetNodes, neededReplicas);
            
        }
        return taskCount;
    }
    
    
    /**
     * uses the authoritativeMemberNode of the object to determine which versions
     * of MNReplication are supported. 
     * @param pid
     * @param authoritativeNode
     * @return
     * @throws InvalidRequest
     */
    protected List<ApiVersion> getSupportedReplicationVersions(Identifier pid, Node authoritativeNode) throws InvalidRequest {
        List<ApiVersion> sourceReplicationSupport = new LinkedList<>();
        boolean sourceSupportsV1Replication = false;
        boolean sourceSupportsV2Replication = false;

        try {
            // calculate supported versions of replication on source
            if (authoritativeNode != null && authoritativeNode.getServices() != null
                    && authoritativeNode.getServices().getServiceList() != null) {
                List<Service> nodeServices = authoritativeNode.getServices()
                        .getServiceList();
                for (Service service : nodeServices) {
                    if (service.getName().equals("MNRead") && service.getAvailable()) {
                        log.info("for pid: " + pid.getValue() + " source MN: "
                                + authoritativeNode.getIdentifier().getValue()
                                + " service info: " + service.getName() + " "
                                + service.getVersion());
                        if (sourceSupportsV1Replication == false) {
                            sourceSupportsV1Replication = ApiVersion.isV1(service
                                    .getVersion());
                        }
                        if (sourceSupportsV2Replication == false) {
                            sourceSupportsV2Replication = ApiVersion.isV2(service
                                    .getVersion());
                        }
                    }
                }
            }

        } catch (NullPointerException npe) {
            throw new InvalidRequest("1080", "Object " + pid.getValue()
                    + " has no authoritative Member Node in its SystemMetadata");
        }

        if (sourceSupportsV1Replication) {
            sourceReplicationSupport.add(ApiVersion.V1);
            log.info("for pid: " + pid.getValue() + " source MN: "
                    + authoritativeNode.getIdentifier().getValue()
                    + " supports V1 replication.");
        } else {
            log.info("for pid: " + pid.getValue() + " source MN: "
                    + authoritativeNode.getIdentifier().getValue()
                    + " DOES NOT support V1 replication.");
        }

        if (sourceSupportsV2Replication) {
            sourceReplicationSupport.add(ApiVersion.V2);
            log.info("for pid: " + pid.getValue() + " source MN: "
                    + authoritativeNode.getIdentifier().getValue()
                    + " supports V2 replication.");
        } else {
            log.info("for pid: " + pid.getValue() + " source MN: "
                    + authoritativeNode.getIdentifier().getValue()
                    + " DOES NOT support V2 replication.");
        }
        return sourceReplicationSupport;
    }
    
    
    /**
     * determines which nodes are potential target nodes for the pid.
     * @param nodeList
     * @param smd
     * @param sourceReplicationSupport
     * @return
     * @throws ServiceFailure
     * @throws NotFound
     */
    protected List<NodeReference> getPotentialTargetNodes(Set<NodeReference> nodeList, 
            SystemMetadata smd, List<ApiVersion> sourceReplicationSupport) 
            throws ServiceFailure, NotFound {
        
        List<NodeReference> potentialTargetNodes = new ArrayList<>();
        
        for (NodeReference nodeReference : nodeList) {

            Identifier pid = smd.getIdentifier();
            Node node = nodeService.getNode(nodeReference);
            // only add MNs as targets, excluding the authoritative MN
            // and MNs that are not tagged to replicate
            if ((node.getType() == NodeType.MN)
                    && node.isReplicate()
                    && !node.getIdentifier().getValue()
                    .equals(smd.getAuthoritativeMemberNode().getValue())
                    && isUnderReplicationAttemptsPerDay(pid, nodeReference)) {

                boolean targetSupportsV1 = false;
                boolean targetSupportsV2 = false;

                if (node != null && node.getServices() != null
                        && node.getServices().getServiceList() != null) {
                    for (Service service : node.getServices().getServiceList()) {
                        if (service.getName().equals("MNReplication")
                                && service.getAvailable()) {

                            log.info("for pid: " + pid.getValue() + " target MN: "
                                    + node.getIdentifier().getValue()
                                    + " service info: " + service.getName() + " "
                                    + service.getVersion());

                            if (ApiVersion.isV1(service.getVersion())
                                    && targetSupportsV1 == false) {
                                targetSupportsV1 = true;
                            }
                            if (ApiVersion.isV2(service.getVersion())
                                    && targetSupportsV2 == false) {
                                targetSupportsV2 = true;
                            }
                        }
                    }
                }

                // logging block
                if (log.isInfoEnabled()) {
                    if (targetSupportsV1) {
                        log.info("for pid: " + pid.getValue() + " target MN: "
                                + node.getIdentifier().getValue()
                                + " supports V1 replication.");
                    } else {
                        log.info("for pid: " + pid.getValue() + " target MN: "
                                + node.getIdentifier().getValue()
                                + " DOES NOT support V1 replication.");
                    }

                    if (targetSupportsV2) {
                        log.info("for pid: " + pid.getValue() + " target MN: "
                                + node.getIdentifier().getValue()
                                + " supports V2 replication.");
                    } else {
                        log.info("for pid: " + pid.getValue() + " target MN: "
                                + node.getIdentifier().getValue()
                                + " DOES NOT support V2 replication.");
                    }
                }

                ApiVersion targetApiVersion = null;
                // if source supports v2 replication, can only replicate to v2 targets
                if (sourceReplicationSupport.contains(ApiVersion.V2)) {
                    if (targetSupportsV2) {
                        targetApiVersion = ApiVersion.getV2Version();
                    }
                } 
                else if (targetSupportsV1
                        && sourceReplicationSupport.contains(ApiVersion.V1)) {
                    targetApiVersion = ApiVersion.getV1Version();
                }

                 
                if (targetApiVersion == null) {
                    log.info("target node: " + node.getIdentifier().getValue()
                            + " does not share an api version with source node: "
                            + smd.getAuthoritativeMemberNode().getValue());
                } else {
                    log.info("for pid:" + pid.getValue() + " target node: "
                            + node.getIdentifier().getValue() + " api "
                            + targetApiVersion.getApiLabel() + " was chosen.");
                    potentialTargetNodes.add(node.getIdentifier());
                }
            }
        }
        return potentialTargetNodes;
    }


    /*
     * Go down the prioritized list of potential target nodes and create tasks
     * for each node until the desired number of replicas have been requested.
     */
    private int createAndQueueTasks(Identifier pid, List<NodeReference> prioritizedNodes, int desiredReplicas) 
            throws ServiceFailure, NotFound {
        
        
        
        int taskCount = 0;
        Node targetNode = null;
        // for each node in the potential node list up to the desired replicas
        // (less the pending/completed replicas)
        for (int j = 0; j < desiredReplicas; j++) {

            log.debug("Evaluating item " + j + " of " + desiredReplicas
                    + " in the potential node list.");
            NodeReference potentialNode = prioritizedNodes.get(j);

            targetNode = this.nodeService.getNode(potentialNode);
            log.debug("currently evaluating " + targetNode.getIdentifier().getValue()
                    + " for task creation for identifier " + pid.getValue());

            // a replica doesn't exist. add it
            boolean updated = false;
            Replica replicaMetadata = new Replica();
            replicaMetadata.setReplicaMemberNode(targetNode.getIdentifier());
            replicaMetadata.setReplicationStatus(ReplicationStatus.QUEUED);
            replicaMetadata.setReplicaVerified(Calendar.getInstance().getTime());

            SystemMetadata sysmeta = null;
            try {
                // refresh sysmeta to avoid VersionMismatch
                sysmeta = this.systemMetadataMap.get(pid);
                updated = this.cnReplication.updateReplicationMetadata(null, pid,
                        replicaMetadata, sysmeta.getSerialVersion().longValue());

            } catch (VersionMismatch e) {

                // retry if the serialVersion is wrong
                try {
                    sysmeta = this.systemMetadataMap.get(pid);
                    updated = this.cnReplication.updateReplicationMetadata(null, pid,
                            replicaMetadata, sysmeta.getSerialVersion().longValue());

                } catch (VersionMismatch e1) {
                    String msg = "Couldn't get the correct serialVersion to update "
                            + "the replica metadata for identifier " + pid.getValue()
                            + " and target node "
                            + targetNode.getIdentifier().getValue();
                    log.error(msg);

                } catch (BaseException be) {
                    // the replica has already completed from a different task
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
                        // something is very wrong with CN self communication
                        // try the round robin address multiple times
                        updated = replicationService.updateReplicationMetadata(pid,
                                replicaMetadata);

                    }

                }

            } catch (BaseException be) {
                // the replica has already completed from a different task
                if (be instanceof InvalidRequest) {
                    log.warn(
                            "Couldn't update replication metadata to "
                                    + replicaMetadata.getReplicationStatus().toString()
                                    + ", it may have possibly already been updated for identifier "
                                    + pid.getValue() + " and target node "
                                    + replicaMetadata.getReplicaMemberNode().getValue()
                                    + ". The error was: " + be.getMessage(), be);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(be);

                    }
                    // something is very wrong with CN self communication
                    // try the round robin address multiple times
                    updated = replicationService.updateReplicationMetadata(pid,
                            replicaMetadata);

                }

            } catch (RuntimeException re) {
                log.error(
                        "Couldn't get system metadata for identifier " + pid.getValue()
                        + " while trying to update replica "
                        + "metadata entry for node "
                        + replicaMetadata.getReplicaMemberNode().getValue(), re);

            }
            if (updated) {
                taskCount++;
                requeueReplicationTask(pid);
                this.replicationTaskQueue.processAllTasksForMN(targetNode
                        .getIdentifier().getValue());
            } else {
                log.error("CN.updateReplicationMetadata() failed for " + "identifier "
                        + pid.getValue() + ", node "
                        + targetNode.getIdentifier().getValue() + ". Task not created.");
            }
        } // end for()
        return taskCount;
    }

    private void removeReplicationTasksForPid(Identifier pid) {
        if (pid != null) {
            log.info("removing replication tasks for pid: " + pid.getValue());
            List<ReplicationTask> tasks = taskRepository.findByPid(pid.getValue());
            taskRepository.delete(tasks);
        }
    }

    private void requeueReplicationTask(Identifier pid) {
        List<ReplicationTask> taskList = taskRepository.findByPid(pid.getValue());
        if (taskList.size() == 1) {
            ReplicationTask task = taskList.get(0);
            if (ReplicationTask.STATUS_IN_PROCESS.equals(task.getStatus())) {
                task.markNew();
                taskRepository.save(task);
            }
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
            sysmeta = this.systemMetadataMap.get(pid);
            policy = sysmeta.getReplicationPolicy();
            isAllowed = policy.getReplicationAllowed().booleanValue();

        } catch (NullPointerException e) {
            log.debug("NullPointerException caught in ReplicationManager.isAllowed() "
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
        SystemMetadata sysmeta = this.systemMetadataMap.get(pid);
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
                node = this.nodeService.getNode(nodeId);
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
     * replication status tables on the CNs and push the results into a dedicated
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
        this.nodeReplicationStatusMap.putAll(countsByNodeStatusMap);
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
