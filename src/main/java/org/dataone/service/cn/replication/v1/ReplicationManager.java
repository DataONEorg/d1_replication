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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.base.RuntimeInterruptedException;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
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
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.Service;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.service.cn.v1.CNReplication;

/**
 * A DataONE Coordinating Node implementation which
 * manages replication queues and executes replication tasks. The service is
 * a Hazelcast cluster member and client and listens for queued replication tasks. 
 * Queued tasks are popped from the queue on a first come first serve basis,
 * and are executed in a distributed manner (not necessarily on the node that
 * popped the task).
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
  
  /* The instance of the IdGenerator created by Hazelcast used 
     to generate "task-ids" */
  private IdGenerator taskIdGenerator;
  
  /* The name of the DataONE Hazelcast cluster group */
  private String groupName;

  /* The name of the DataONE Hazelcast cluster password */
  private String groupPassword;
  
  /* The name of the DataONE Hazelcast cluster IP addresses */
  private String addressList;
  
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
  
  /* The Hazelcast distributed replication tasks queue*/
  private IQueue<MNReplicationTask> replicationTasks;

  /* The Replication task thread queue */
  private BlockingQueue<Runnable> taskThreadQueue;
  
  /* The handler instance for rejected tasks */
  private RejectedExecutionHandler handler;
  
  /* The thread pool executor instance for executing tasks */
  private ThreadPoolExecutor executor;
  
  /* The event listener used to manage incoming map and queue changes */
  private ReplicationEventListener listener;
  
  /* The timeout period for tasks submitted to the executor service 
   * to complete the call to MN.replicate()
   */
  private long timeout = 30L;
  
  /* A client reference to the coordinating node */
  private CNReplication cnReplication = null;
    
  /**
   *  Constructor - singleton pattern
   */
  public ReplicationManager() {
    
    this.nodeMap = 
      Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    this.systemMetadataMap = 
      Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    this.tasksQueue = 
      Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
    this.taskIds = 
      Settings.getConfiguration().getString("dataone.hazelcast.tasksIdGenerator");
    this.hzAuditString = 
      Settings.getConfiguration().getString("dataone.hazelcast.auditString");
    this.shortListAge = 
      Settings.getConfiguration().getString("dataone.hazelcast.shortListAge");
    this.shortListNumRows = 
      Settings.getConfiguration().getString("dataone.hazelcast.shortListNumRows");
    
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
    
    // initialize the task thread queue with 5 threads
    taskThreadQueue = new ArrayBlockingQueue<Runnable>(5);
    handler = new RejectedReplicationTaskHandler();
    // create an Executor with 8 core, 8 max threads, 30s timeout
    executor = 
        new ThreadPoolExecutor(8, 8, 30, TimeUnit.SECONDS, taskThreadQueue, handler);
    executor.allowCoreThreadTimeOut(true);

    // Set up the certificate location, create a null session
    String clientCertificateLocation =
            Settings.getConfiguration().getString("D1Client.certificate.directory")
            + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
    CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
    log.info("ReplicationManager is using an X509 certificate from " + clientCertificateLocation);
    
  }

  public void init() {
      log.info("initialization");
      CNode cnode = null;
    // Get an CNode reference to communicate with
    try {
        log.debug("D1Client.CN_URL = " +
            Settings.getConfiguration().getProperty("D1Client.CN_URL"));

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
            log.error("There was a problem getting a Coordinating Node reference " +
                " for the ReplicationManager. ");
            e1.printStackTrace();
            throw new RuntimeException(e1);

        }
    }
    this.cnReplication = cnode;
  }
  /**
   * Create replication tasks given the identifier of an object
   * by evaluating its system metadata and the capabilities of the target
   * replication nodes. Queue the tasks for processing.
   * 
   * @param pid - the identifier of the object to be replicated
   * @return count - the number of replication tasks queued
   * 
   * @throws ServiceFailure
   * @throws NotImplemented
   * @throws InvalidToken
   * @throws NotAuthorized
   * @throws InvalidRequest
   * @throws NotFound
   */
  public int createAndQueueTasks(Identifier pid) 
    throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
    InvalidRequest, NotFound {

    log.info("ReplicationManager.createAndQueueTasks called.");
    boolean allowed;
    int taskCount = 0;
    int desiredReplicas;
    List<Replica> replicaList;             // the replica list for this pid
    Set<NodeReference> nodeList;           // the full nodes list
    List<NodeReference> potentialNodeList; // the MN subset of the nodes list
    Node targetNode = new Node();          // the target node for the replica
    Node authoritativeNode = new Node();   // the source node of the object
    SystemMetadata sysmeta;
    Session session = null;


    
    // if replication isn't allowed, return
    allowed = isAllowed(pid);
    log.info("Replication is allowed for identifier " + pid.getValue());
    
    if ( !allowed ) {
      log.info("Replication is not allowed for the object identified by " +
          pid.getValue());
      return 0;
      
    }

    boolean no_task_with_pid = true;

    // check if there are pending tasks or tasks recently put into the task queue
    if ( !isPending(pid) ) {
        log.debug("Replication is not pending for identifier " + pid.getValue());
        // check for already queued tasks for this pid
        for (MNReplicationTask task : replicationTasks) {
            // if the task's pid is equal to the event's pid
            if (task.getPid().getValue().equals(pid.getValue())) {
                no_task_with_pid = false;
                log.debug("An MNReplicationTask is already queued for identifier " + pid.getValue());
                break;
                
            }
        }
        if ( !no_task_with_pid ) {
            log.info("A replication task for the object identified by " +
                pid.getValue() + " has already been queued.");
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
    potentialNodeList = new ArrayList<NodeReference>(); // will be our short list
    
    // authoritative member node to replicate from
    try {
        authoritativeNode = this.nodes.get(sysmeta.getAuthoritativeMemberNode());
        
    } catch (NullPointerException npe) {
        throw new InvalidRequest("1080", "Object " + pid.getValue() + 
                " has no authoritative Member Node in its SystemMetadata");
        
    }
    
    // build the potential list of target nodes
    for(NodeReference nodeReference : nodeList) {
      Node node = this.nodes.get(nodeReference);
      
        // only add MNs as targets, excluding the authoritative MN and MNs that are not tagged to replicate
        if ( (node.getType() == NodeType.MN) && node.isReplicate() &&
            !node.getIdentifier().getValue().equals(authoritativeNode.getIdentifier().getValue())) {
          potentialNodeList.add(node.getIdentifier());
          
        }
    }

    // parse the sysmeta.ReplicationPolicy
    ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();
    desiredReplicas = replicationPolicy.getNumberReplicas().intValue();

    // get the preferred and blocked lists for prioritization
    List<NodeReference> preferredList = 
        replicationPolicy.getPreferredMemberNodeList();
    List<NodeReference> blockedList = 
      replicationPolicy.getBlockedMemberNodeList();

    log.info("Removing blocked nodes from the potential replication list for " +
      pid.getValue());
    // remove blocked nodes from the potential nodelist
    if ( blockedList != null && !blockedList.isEmpty() ) {
      for (NodeReference blockedNode : blockedList) {
        if ( potentialNodeList.contains(blockedNode) ) {
          potentialNodeList.remove(blockedNode);
          
        }
      }
    }

    log.info("Prioritizing preferred nodes in the potential replication list for " +
      pid.getValue());

    // prioritize preferred nodes in the potential node list
    if ( preferredList != null && !preferredList.isEmpty() ) {
      
      // process preferred nodes backwards
      for (int i = preferredList.size() - 1; i >= 0; i--) {
        
        NodeReference preferredNode = preferredList.get(i);
        if ( potentialNodeList.contains( preferredNode) ) {
          potentialNodeList.remove(preferredNode); // remove for reordering
          potentialNodeList.add(0, preferredNode); // to the top of the list
           
        }
      }
    }
     
    log.info("Desired replicas for identifier " + pid.getValue() + " is " + desiredReplicas);
    log.info("Potential target node list size for " + pid.getValue() + " is " + desiredReplicas);

    // can't have more replicas than MNs
    if ( desiredReplicas > potentialNodeList.size() ) {
      desiredReplicas = potentialNodeList.size(); // yikes
      log.info("Changed the desired replicas for identifier " + pid.getValue() +
        " to the size of the potential target node list: "  + potentialNodeList.size());
              
    }
    
    boolean alreadyAdded = false;

    // for each node in the potential node list up to the desired replicas
    for(int j = 0; j < desiredReplicas; j++) {

      NodeReference potentialNode = potentialNodeList.get(j);
      // for each replica in the replica list
      for (Replica replica : replicaList) {
        
        // ensure that this node is not queued, requested, failed, or completed
        NodeReference replicaNode = replica.getReplicaMemberNode();
        ReplicationStatus status = replica.getReplicationStatus();
        if (potentialNode.getValue().equals(replicaNode.getValue()) &&
           (status.equals(ReplicationStatus.QUEUED) ||
            status.equals(ReplicationStatus.REQUESTED) ||
            status.equals(ReplicationStatus.FAILED) ||
            status.equals(ReplicationStatus.COMPLETED))){
          alreadyAdded = true;
          break;
          
        }
        log.info("A replication task for identifier " + pid.getValue() + 
          " on node id " + replicaNode.getValue() +
          " has already been added: " + alreadyAdded + ". The status is " +
          "currently set to: " + status);
        
      }
      // if the node doesn't already exist as a pending task for this pid
      if ( !alreadyAdded ) {
        targetNode = this.nodes.get(potentialNode);
              
      } else {
          // skip on to the next one right? -rpw (otherwise targetnode is empty or is the last targetnode assigned)
          continue;
      }
      
      boolean replicaAdded = false;
        
      // may be more than one version of MNReplication
      List<String> implementedVersions = new ArrayList<String>();
      List<Service> origServices = authoritativeNode.getServices().getServiceList();
      for (Service service : origServices) {
          if(service.getName().equals("MNReplication") &&
             service.getAvailable()) {
              implementedVersions.add(service.getVersion());
          }
      }
      if (implementedVersions.isEmpty()) {
          throw new InvalidRequest("1080","Authoritative Node:" + authoritativeNode.getIdentifier().getValue() + " MNReplication Service is not available or is missing version");
      }

      boolean replicable = false;

      for (Service service : targetNode.getServices().getServiceList()) {
          if(service.getName().equals("MNReplication") &&
             implementedVersions.contains(service.getVersion()) &&
             service.getAvailable()) {
              replicable = true;
          }
      }
      log.info("Based on evaluating the target node services, node id " +
              targetNode.getIdentifier().getValue() + " is replicable: " + 
              replicable + " (during evaluation for " + pid.getValue() + ")");

      // a replica doesn't exist. add it
      if ( replicable ) {
          Replica replicaMetadata = new Replica();
          replicaMetadata.setReplicaMemberNode(targetNode.getIdentifier());
          replicaMetadata.setReplicationStatus(ReplicationStatus.QUEUED);
          replicaMetadata.setReplicaVerified(Calendar.getInstance().getTime());
                  
          try {
              sysmeta = this.systemMetadata.get(pid); // refresh sysmeta to avoid VersionMismatch
              this.cnReplication.updateReplicationMetadata(session, pid,
                      replicaMetadata, sysmeta.getSerialVersion().longValue());
          
          } catch (VersionMismatch e) {
              
              // retry if the serialVersion is wrong
              try {
                  sysmeta = this.systemMetadata.get(pid); 
                  this.cnReplication.updateReplicationMetadata(session, pid,
                          replicaMetadata, sysmeta.getSerialVersion().longValue());
                  
              } catch (VersionMismatch e1) {
                  String msg = "Couldn't get the correct serialVersion to update " + 
                      "the replica metadata for identifier " + pid.getValue() +
                      " and target node " + targetNode.getIdentifier().getValue();
                  log.info(msg);
                
              }
              
          }
                    
          log.info("Updated system metadata for identifier " + pid.getValue() + 
                  " with QUEUED replication status.");
          
          Long taskid = taskIdGenerator.newId();
          // add the task to the task list
          log.info("Adding a new MNReplicationTask to the queue where " +
            "pid = "               + pid.getValue() + 
            ", originatingNode = " + authoritativeNode.getIdentifier().getValue() +
            ", targetNode = "      + targetNode.getIdentifier().getValue());
          
          MNReplicationTask task = new MNReplicationTask(
                              taskid.toString(),
                              pid,
                              authoritativeNode.getIdentifier(),
                              targetNode.getIdentifier());
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
  public void auditReplicas() {
      try{
          // get lock on hzAuditString
          Hazelcast.getLock(this.hzAuditString);

          CNode cn = null;

          log.info("Getting the CNode reference for the audit list query.");
          cn = D1Client.getCN();

          // call against the SOLR Indexer to receive a short list of identifiers
          // which have replicas unchecked in > shortListAge
          SolrDocumentList shortList = 
              cn.getAuditShortList(this.shortListAge, 
                                   Integer.parseInt(this.shortListNumRows));

          SystemMetadata sysmeta = null;

          // bin the tasks by NodeReference for bulk processing by MNAuditTask
          for (SolrDocument doc : shortList) {
              sysmeta = this.systemMetadata.get(doc.get("id"));
              for (Replica replica : sysmeta.getReplicaList()) {
                  if (replica.getReplicaVerified()) {
                      //
                  } else {
                      //
                  }
              }
          }
      } catch (ServiceFailure sf) {
          log.error("Failed to get the CNode for the audit list query.");
      } catch (SolrServerException sse) {
          log.error("Failed to perform query on SOLR Index for audit short list");
      }
  }
  */

  /**
   * Implement the ItemListener interface, responding to items being added to
   * the hzReplicationTasks queue.
   * 
   * @param task - the MNReplicationTask being added to the queue
   */
  public void itemAdded(MNReplicationTask task) {

    // When a task is added to the queue, attempt to handle the task. If 
    // successful, execute the task.    
    try {
      task = this.replicationTasks.poll(3L, TimeUnit.SECONDS);
      
      if ( task != null ) {
          log.info("Submitting replication task id " + task.getTaskid() +
                   " for execution with object identifier: " + task.getPid().getValue());
          
          // TODO: handle the case when a CN drops and the MNReplicationTask.call()
          // has not been made.
          FutureTask<String> futureTask = new FutureTask<String>(task);
          executor.execute(futureTask);
          //ExecutorService executorService = 
          //     this.hzMember.getExecutorService("ReplicationTasks");
          //executorService.submit(futureTask);
          //Future<?> future = executorService.submit(new DistributedTask(task));
          log.debug("Task thread queue has "  + 
              taskThreadQueue.remainingCapacity() + " slots available.");
          log.debug("ExecutorService is shut down: " + executor.isShutdown());          
          log.debug("ExecutorService allows core thread timeout: " + 
              executor.allowsCoreThreadTimeOut());
          log.debug("ExecutorService is currently executing " + 
              executor.getActiveCount() + " tasks.");
          log.debug("ExecutorService has executed " + 
              executor.getCompletedTaskCount() + " tasks.");
          log.debug("ExecutorService core pool size is " + 
              executor.getCorePoolSize() + " threads.");
          log.debug("ExecutorService currently has " + 
              executor.getPoolSize() + " threads.");
          log.debug("ExecutorService has max simultaneously executed " +
              executor.getLargestPoolSize() + " threads.");
          log.debug("ExecutorService has scheduled " +
              executor.getTaskCount() + " tasks.");
          
          // check for completion
          boolean isDone = false;
          String result = null;
          
          while( !isDone ) {
                              
              try {
                  result = (String) futureTask.get(15L, TimeUnit.SECONDS);  
                  log.trace("Task result for identifier " + 
                      task.getPid().getValue() + " is " + result );
                  if ( result != null ) {
                      log.debug("Task " + task.getTaskid() + " completed for identifier " +
                          task.getPid().getValue());
                      
                  }
                  
              } catch (ExecutionException e) {
                  String msg = e.getCause().getMessage();
                  log.info("MNReplicationTask id " + task.getTaskid() +
                      " threw an execution execptionf or identifier " +
                          task.getPid().getValue() + ": " + msg);
                  if ( task.getRetryCount() < 10 ) {
                      task.setRetryCount(task.getRetryCount() + 1);
                      futureTask.cancel(true);
                      this.replicationTasks.add(task);
                      log.info("Retrying replication task id " + 
                          task.getTaskid() + " for identifier " + 
                          task.getPid().getValue());
                      
                  } else {
                      log.info("Replication task id" + task.getTaskid() + 
                          " failed, too many retries for identifier" + 
                          task.getPid().getValue() + ". Not retrying.");
                  }
                  
              } catch (TimeoutException e) {
                  String msg = e.getMessage();
                  log.info("Replication task id " + task.getTaskid() + 
                          " timed out for identifier " + task.getPid().getValue() +
                          " : " + msg);
                  futureTask.cancel(true); // isDone() is now true
                  //if ( task.getRetryCount() < 10 ) {
                  //    task.setRetryCount(task.getRetryCount() + 1);
                  //    this.replicationTasks.add(task);
                  //    log.info("Retrying replication task id " + 
                  //        task.getTaskid() + " for identifier " + 
                  //        task.getPid().getValue());
                  //    
                  //} else {
                  //    log.info("Replication task id" + task.getTaskid() + 
                  //        " failed, too many retries for identifier" + 
                  //        task.getPid().getValue() + ". Not retrying.");
                  //}
                  
              } catch (InterruptedException e) {
                  String msg = e.getMessage();
                  log.info("Replication task id " + task.getTaskid() + 
                          " was interrupted for identifier " + task.getPid().getValue() +
                          " : " + msg);
                  if ( task.getRetryCount() < 10 ) {
                      task.setRetryCount(task.getRetryCount() + 1);
                      this.replicationTasks.add(task);
                      log.info("Retrying replication task id " + 
                          task.getTaskid() + " for identifier " + 
                          task.getPid().getValue());
                      
                  } else {
                      log.error("Replication task id" + task.getTaskid() + 
                          " failed, too many retries for identifier" + 
                          task.getPid().getValue() + ". Not retrying.");
                  }

              }
              
              isDone = futureTask.isDone();
              log.debug("Task " + task.getTaskid() + " is done for identifier " +
                      task.getPid().getValue() + ": " + isDone);
              
              
              // handle canceled tasks (from the timeout period)
              if ( futureTask.isCancelled() ) {
                  log.info("Replication task id " + task.getTaskid() + 
                           " was cancelled for identifier " + task.getPid().getValue());
                  
                  // leave the Replica entry as QUEUED in system metadata
                  // to be picked up later
              }
          }
            
      }
    
    } catch (InterruptedException e) {

      String message = "Polling of the replication task queue was interrupted. " +
                       "The message was: " + e.getMessage();
      log.info(message);
    } catch (RuntimeInterruptedException rie) {
        String message = "Hazelcast instance was lost due to cluster shutdown, " +
            rie.getMessage();
        
    } catch (IllegalStateException ise) {
        String message = "Hazelcast instance was lost due to cluster shutdown, " +
            ise.getMessage();
    }
      
 }
  
  /**
   * Implement the ItemListener interface, responding to items being removed from
   * the hzReplicationTasks queue.
   * 
   * @param task - the object being removed from the queue (ReplicationTask)
   */
  public void itemRemoved(MNReplicationTask task) {
    // not implemented until needed
    
    
  }
  
  /**
   * Check if replication is allowed for the given pid
   * @param pid - the identifier of the object to check
   * @return
   */
  public boolean isAllowed(Identifier pid) {
    boolean isAllowed = false;
    SystemMetadata sysmeta = this.systemMetadata.get(pid);
    ReplicationPolicy policy = sysmeta.getReplicationPolicy();
    
        try {
            isAllowed = policy.getReplicationAllowed().booleanValue();
        
        } catch (NullPointerException e) {
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
      if (status.equals(ReplicationStatus.QUEUED) || 
          status.equals(ReplicationStatus.REQUESTED)) {
        is_pending = true;
        break;

      }
        
    }

    return is_pending;
  }

    public void setCnReplication(CNReplication cnReplication) {
        this.cnReplication = cnReplication;
    }
  
}
