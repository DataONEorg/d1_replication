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
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ItemListener;
import com.hazelcast.query.SqlPredicate;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;

import org.dataone.client.D1Client;
import org.dataone.client.CNode;
import org.dataone.client.MNode;

/**
 * A DataONE Coordinating Node implementation of the CNReplication API which
 * manages replication queues and executes replication tasks. The service is
 * also a Hazelcast cluster member and listens for queued replication tasks. 
 * Queued tasks are popped from the queue on a first come first serve basis,
 * and are executed in a distributed manner (not necessarily on the node that
 * popped the task).
 * 
 * @author cjones
 *
 */
public class ReplicationService implements CNReplication, 
  EntryListener<Identifier, SystemMetadata>, ItemListener<ReplicationTask> {

  /* Get a Log instance */
  public static Log log = LogFactory.getLog(ReplicationService.class);
  
  /* The instance of the Hazelcast client */
  private HazelcastClient hzClient;

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
  
  /* The name of the pending replication tasks map */
  private String pendingTasksQueue;
  
  /* The Hazelcast distributed task id generator namespace */
  private String taskIds;
  
  /* The Hazelcast distributed system metadata map */
  private IMap<NodeReference, Node> nodes;

  /* The Hazelcast distributed system metadata map */
  private IMap<Identifier, SystemMetadata> systemMetadata;
  
  /* The Hazelcast distributed replication tasks queue*/
  private IQueue<ReplicationTask> replicationTasks;
  
  /* The Hazelcast distributed pending replication tasks map*/
  private IMap<String, ReplicationTask> pendingReplicationTasks;

  /**
   * Private Constructor - singleton pattern
   */
  public ReplicationService() {
    
    // Get configuration properties on instantiation
    this.groupName = 
      Settings.getConfiguration().getString("dataone.hazelcast.group");
    this.groupPassword = 
      Settings.getConfiguration().getString("dataone.hazelcast.password");
    this.addressList = 
      Settings.getConfiguration().getString("dataone.hazelcast.clusterInstances");
    this.nodeMap = 
      Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    this.systemMetadataMap = 
      Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
    this.tasksQueue = 
      Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
    this.pendingTasksQueue = 
      Settings.getConfiguration().getString("dataone.hazelcast.replicationPendingTasks");
    this.taskIds = 
      Settings.getConfiguration().getString("dataone.hazelcast.tasksIdGenerator");
    
    // Become a Hazelcast cluster client using the replication structures
    String[] addresses = this.addressList.split(",");
    this.hzClient = 
      HazelcastClient.newHazelcastClient(this.groupName, this.groupPassword, addresses);
    this.nodes = this.hzClient.getMap(nodeMap);
    this.systemMetadata = this.hzClient.getMap(systemMetadataMap);
    this.replicationTasks = this.hzClient.getQueue(tasksQueue);
    this.pendingReplicationTasks = this.hzClient.getMap(pendingTasksQueue);
    this.taskIdGenerator = this.hzClient.getIdGenerator(taskIds);
    
    // monitor the replication structures
    this.systemMetadata.addEntryListener(this, true);
    this.replicationTasks.addItemListener(this, true);
  }

      
  /**
   * Update the replication policy entry for an object by updating the system metadata.
   *
   * @param session - Session information that contains the identity of the calling user
   * @param pid - Identifier of the object to be replicated between Member Nodes
   * @param status - Replication policy. See the replication policy schema.
   * @return true if setting the status succeeds
   * 
   * @throws ServiceFailure
   * @throws NotImplemented
   * @throws InvalidToken
   * @throws NotAuthorized
   * @throws InvalidRequest
   * @throws NotFound
   */
  public boolean setReplicationPolicy(Session session, Identifier pid,
    ReplicationPolicy replicationPolicy) 
    throws NotImplemented, NotFound, NotAuthorized,
      ServiceFailure, InvalidRequest, InvalidToken {

  	Subject authoritativeNodeSubject;
  	Node authoritativeNode;
  	Subject callingNodeSubject;
  	Node callingNode;
  	
    return false;
  }

  /**
   * Update the replication status of the system metadata, ensuring that the 
   * change is appropriate for the given state of system metadata
   * 
   * @param session - Session information that contains the identity of the calling user
   * @param pid - Identifier of the object to be replicated between Member Nodes
   * @param status - Replication status. See system metadata schema for possible values.
   * @return true if setting the status succeeds
   * 
   * @throws ServiceFailure
   * @throws NotImplemented
   * @throws InvalidToken
   * @throws NotAuthorized
   * @throws InvalidRequest
   * @throws NotFound
   */
  public boolean setReplicationStatus(Session session, Identifier pid,
    NodeReference nodeRef, ReplicationStatus replicationStatus) 
    throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
    InvalidRequest, NotFound {
    
    boolean success = false; // did the system metadata change succeeed?
    boolean checksumIsValid = false;
    SystemMetadata sysMeta;
    List<Replica> replicaList;
    NodeReference replicaNodeId;
    String replicaNodeSubject;
    
    // Get the replica node subject for later use
    Node replicaNode = this.nodes.get(nodeRef);
    replicaNodeSubject = replicaNode.getSubject(0).getValue();
    
    // What type of Node is calling? If it's an MN, verify the checksum
    NodeType callingNodeType = null;
    Subject subject = session.getSubject();
    List<Node> nodeList = (List<Node>) this.nodes.values();

    // find the node identified by the subject
    for (Node node : nodeList) {
      List<Subject> subjectList = node.getSubjectList();
      
      if ( subjectList.contains(subject) ) {
        callingNodeType = node.getType();
        
      }
    }

    // verify checksums for MNs only 
    if ( callingNodeType == NodeType.MN ) {
      
      //is it the target MN? If not, don't allow it
      if ( !isNodeAuthorized(session, session.getSubject(), pid, Permission.REPLICATE)) {
        throw new NotAuthorized("4871",  "The calling node identified by " +
          session.getSubject().getValue() +
          " isn't authorized to set the replication status for the object" +
          " identified by " + pid.getValue());
        
      }
      
      // verify the checksum if this is the final status
      this.systemMetadata.lock(pid);
      sysMeta = this.systemMetadata.get(pid);
      this.systemMetadata.unlock(pid);

      if (replicationStatus == ReplicationStatus.COMPLETED) {
        
        checksumIsValid = 
          verifyChecksum(sysMeta.getAuthoritativeMemberNode(), nodeRef, pid, 
            sysMeta.getChecksum().getAlgorithm());
      
      } else if (replicationStatus == ReplicationStatus.REQUESTED || 
      		       replicationStatus == ReplicationStatus.QUEUED ||
      		       replicationStatus == ReplicationStatus.INVALIDATED) {
        throw new NotAuthorized("4720", "Only Coordinating Nodes are currently " +
        	"allowed to set replication status to " + replicationStatus.toString() );        
        
      }
      
      if ( checksumIsValid ) {
        replicationStatus = ReplicationStatus.COMPLETED;        

      } else {
        replicationStatus = ReplicationStatus.INVALIDATED;        
        log.error("The checksums don't match for " + pid.getValue() +
          "on the source node (" + sysMeta.getAuthoritativeMemberNode().getValue() +
          ") and the target node (" + nodeRef.getValue() + ").");
      }
      
      // get the system metadata again in case it changed
      this.systemMetadata.lock(pid);
      sysMeta = this.systemMetadata.get(pid);
      this.systemMetadata.unlock(pid);
      
      // get the correct replica that is already listed
      replicaList = sysMeta.getReplicaList();
      for (Replica replica : replicaList) {
        replicaNodeId = replica.getReplicaMemberNode();
        
        if (replicaNodeId == nodeRef ) {
          // update the status in the list
          replica.setReplicationStatus(replicationStatus);
          sysMeta.setReplicaList(replicaList);
          sysMeta.setDateSysMetadataModified(new Date());
          // update the replica status
          this.systemMetadata.lock(pid);
          this.systemMetadata.put(pid, sysMeta);
          this.systemMetadata.unlock(pid);
          success = true;
          break; // found it, we're done.
          
        }
      }
      
      // remove the task from the pending tasks map
      String query = "pid = " + pid.getValue() + 
       " AND targetNodeSubject = " + replicaNodeSubject + "'";
      
      Set<ReplicationTask> pendingTasks = 
        (Set<ReplicationTask>) this.pendingReplicationTasks.values(new SqlPredicate(query));
      log.debug("For pid " + pid.getValue() + ", found " + pendingTasks.size() +
        " tasks.");
      // TODO: should only be one task, add checks or exceptions
      for ( ReplicationTask task : pendingTasks ) {
        String taskid = task.getTaskid();
        this.pendingReplicationTasks.lock(taskid);
        this.pendingReplicationTasks.remove(taskid);
        log.info("Removed pending task (" + taskid + ") for pid " + pid.getValue());
        this.pendingReplicationTasks.unlock(taskid);
        
      }
    }
    
    try {
      this.systemMetadata.lock(pid);
      sysMeta = this.systemMetadata.get(pid);
      this.systemMetadata.unlock(pid);
    
      if ( sysMeta == null ) {
        // object doesn't exist
        throw new NotFound("4740", "the object identified by " + pid.getValue() +
            " was not found.");
        
      } else {
        // find and update the correct replica's status
        replicaList = sysMeta.getReplicaList();
        
        if ( replicaList.size() == 0 ) {
          
          // no replicas exist. create a new entry
          replicaList = new ArrayList<Replica>();
          Replica newReplica = new Replica();
          newReplica.setReplicaMemberNode(nodeRef);
          newReplica.setReplicationStatus(replicationStatus);
          newReplica.setReplicaVerified(new Date());
          replicaList.add(newReplica);
          sysMeta.setReplicaList(replicaList);
          sysMeta.setDateSysMetadataModified(new Date());

          // update the replica status
          this.systemMetadata.lock(pid);
          this.systemMetadata.put(pid, sysMeta);
          this.systemMetadata.unlock(pid);
          success = true;
          
        } else {
          
          // get the correct replica that is already listed
          for (Replica replica2 : replicaList) {
            replicaNodeId = replica2.getReplicaMemberNode();
            
            if (replicaNodeId == nodeRef ) {
              // update the status in the list
              replica2.setReplicationStatus(replicationStatus);
              sysMeta.setReplicaList(replicaList);
              sysMeta.setDateSysMetadataModified(new Date());
              // update the replica status
              this.systemMetadata.lock(pid);
              this.systemMetadata.put(pid, sysMeta);
              this.systemMetadata.unlock(pid);
              success = true;
              break; // found it, we're done.
              
            }
          }
        }
      }
      
    } catch (Exception e) {

      // catch Hazelcast exceptions and recast them
      throw new ServiceFailure("4700", "An error occurred while setting the " +
        "replication status.  the message was: " + e.getMessage());
      
    }

    return success;
    
  }
  
  /**
   * Create a list of replication tasks given the identifier of an object
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

    // get the system metadata for the pid
    SystemMetadata sysmeta = this.systemMetadata.get(pid);
    
    // the list of replication tasks to create and queue
    List<ReplicationTask> taskList = new ArrayList<ReplicationTask>();

    // CN for getting nodes for tasks
    CNode cn = D1Client.getCN();

    // List of Nodes for building ReplicationTasks
    List<Node> nodes = cn.listNodes().getNodeList();

    // authoritative member node to replicate from
    Node originatingNode = new Node();
    for(Node node : nodes) {
        if(node.getIdentifier().getValue() == 
                sysmeta.getAuthoritativeMemberNode().getValue()) {
            originatingNode = node;
        }
    }

    // parse the sysmeta.ReplicationPolicy
    ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();
    List<NodeReference> preferredList = 
        replicationPolicy.getPreferredMemberNodeList();

    // query the pendingReplicationTasks for tasks pending for this pid
    String query = "";
    query += "pid = '";
    query += pid;
    query += "'";

    log.debug("Pending replication task query is: " + query);

    // perform query
    Set<ReplicationTask> pendingTasksForPID = 
        (Set<ReplicationTask>) 
        this.pendingReplicationTasks.values(new SqlPredicate(query));

    // flag for whether a node is in the preferred list for this pid
    boolean alreadyAdded = false;

    // for each node in the preferred list
    for(NodeReference nodeRef : preferredList) {
        alreadyAdded = false;
        // for each task in the pending tasks
        for(ReplicationTask task : pendingTasksForPID) {
            // ensure that this node is not in the pending tasks list
            if(nodeRef.getValue().equals(
                        task.getTargetNode().getIdentifier().getValue())){
                alreadyAdded = true;
                break;
            }
        }
        Node targetNode = new Node();
        // if the node doesn't already exist in the pendingTasks for this pid
        if(!alreadyAdded) {
            for(Node node : nodes) {
                if(nodeRef.getValue().equals(node.getIdentifier().getValue())) {
                    targetNode = node;
                }
            }
            Long taskid = taskIdGenerator.newId();
            // add the task to the task list
            taskList.add(new ReplicationTask(
                                taskid.toString(),
                                pid,
                                originatingNode,
                                targetNode,
                                Permission.REPLICATE));
        }
    }

    // add list to the queue
    for (ReplicationTask task : taskList) {
        this.replicationTasks.add(task);
    }

    // return the number of replication tasks queued
    return taskList.size();
  }
  
  /**
   * Verify that a replication task is authorized by comparing the target node's
   * Subject (from the X.509 certificate-derived Session) with the list of 
   * subjects in the known, pending replication tasks map.
   * 
   * @param originatingNodeSession - Session information that contains the 
   *                                 identity of the calling user
   * @param targetNodeSubject - Subject identifying the target node
   * @param pid - the identifier of the object to be replicated
   * @param executePermission - the execute permission to be granted
   * 
   * @throws ServiceFailure
   * @throws NotImplemented
   * @throws InvalidToken
   * @throws NotAuthorized
   * @throws InvalidRequest
   * @throws NotFound
   */
  public boolean isNodeAuthorized(Session originatingNodeSession,
    Subject targetNodeSubject, Identifier pid, Permission replicatePermission)
    throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure,
    NotFound, InvalidRequest {

    // build a predicate like: 
    // "pid                    = '{pid}                   ' AND 
    //  pemission              = '{permission}            ' AND
    //  originatingNodeSubject = '{originatingNodeSubject}' AND
    //  targetNodeSubject      = '{targetNodeSubject}     '"
    boolean isAllowed = false;
    String query = "";
    query += "pid = '";
    query += pid;
    query += "' AND permission = '";
    query += replicatePermission.name();
    query += "' AND originatingNodeSubject = '";
    query += originatingNodeSession.getSubject().getValue();
    query += "' AND targetNodeSubject = '";
    query += targetNodeSubject.getValue();
    query += "'";
    
    log.debug("Pending replication task query is: " + query);
    // search the hzPendingReplicationTasks map for the  originating node subject, 
    // target node subject, pid, and replicate permission
    
    Set<ReplicationTask> tasks = 
      (Set<ReplicationTask>) this.pendingReplicationTasks.values(new SqlPredicate(query));
    
    // do we have a matching task?
    if ( tasks.size() >= 1 ) {
      isAllowed = true;
      
    }
    
    return isAllowed;
  }
  
  /**
   * Implement the ItemListener interface, responding to items being added to
   * the hzReplicationTasks queue.
   * 
   * @param task - the ReplicationTask being added to the queue
   */
  public void itemAdded(ReplicationTask task) {

    // When a task is added to the queue, attempt to handle the task. If 
    // successful, execute the task.    
    try {
      task = this.replicationTasks.poll(3, TimeUnit.SECONDS);
      
      if ( task != null ) {
        log.info("Scheduling replication task id " + task.getTaskid() +
                 " for object identifier: " + task.getPid().getValue());
        
        // TODO: handle the case when a CN drops and the ReplicationTask.call()
        // has not been made.
        ExecutorService executorService = this.hzClient.getExecutorService();
        Future<String> replicationTask = executorService.submit(task);
        
        // check for completion
        while ( !replicationTask.isDone() ) {
          
          if ( replicationTask.isCancelled() ) {
            log.info("Replication task id " + task.getTaskid() + 
                     " was cancelled.");
          }
          
        }
        
        log.info("Replication task id " + task.getTaskid() + " completed.");
        
        //TODO: lock pid, update sysmeta to set ReplicationStatus.REQUESTED
      }
    
    } catch (InterruptedException e) {

      String message = "Polling of the replication task queue was interrupted. " +
                       "The message was: " + e.getMessage();
      log.info(message);
    }
      
 }
  
  /**
   * Implement the ItemListener interface, responding to items being removed from
   * the hzReplicationTasks queue.
   * 
   * @param task - the object being removed from the queue (ReplicationTask)
   */
  public void itemRemoved(ReplicationTask task) {
    // not implemented until needed
    
    
  }
  
  /**
   * Implement the EntryListener interface, responding to entries being added to
   * the hzSystemMetadata map.
   * 
   * @param event - the entry event being added to the map
   */
  public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
    
    try {
      
      // try to lock the pid and handle the event (only one ReplicationService
      // instance within the cluster should get the lock)
      boolean locked = 
        this.systemMetadata.tryLock(event.getKey(), 3, TimeUnit.SECONDS);
      if ( locked ) { // TODO stiffen this condition
        this.createAndQueueTasks(event.getKey());
        this.systemMetadata.unlock(event.getKey());
        
      }
      
    } catch (ServiceFailure e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotImplemented e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (InvalidToken e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotAuthorized e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (InvalidRequest e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotFound e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } finally {
      this.systemMetadata.unlock(event.getKey());
      
    }
    
  }
  
  /**
   * Implement the EntryListener interface, responding to entries being deleted from
   * the hzSystemMetadata map.
   * 
   * @param event - the entry event being deleted from the map
   */
  public void entryRemoved(EntryEvent<Identifier, SystemMetadata> event) {
    // we don't remove replicas (do we?) 
    
  }
  
  /**
   * Implement the EntryListener interface, responding to entries being updated in
   * the hzSystemMetadata map.
   * 
   * @param event - the entry event being updated in the map
   */
  public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
    try {
      
      // try to lock the pid and handle the event (only one ReplicationService
      // instance within the cluster should get the lock)
      boolean locked = 
        this.systemMetadata.tryLock(event.getKey(), 3, TimeUnit.SECONDS);
      if ( locked ) {
        this.createAndQueueTasks(event.getKey());
        this.systemMetadata.unlock(event.getKey());
      }
      
    } catch (ServiceFailure e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotImplemented e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (InvalidToken e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotAuthorized e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (InvalidRequest e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } catch (NotFound e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    
    } finally {
      this.systemMetadata.unlock(event.getKey());
      
    }
    
  }
  
  /**
   * Implement the EntryListener interface, responding to entries being evicted from
   * the hzSystemMetadata map.
   * 
   * @param event - the entry event being evicted from the map
   */
  public void entryEvicted(EntryEvent<Identifier, SystemMetadata> event) {
    // nothing to do, entry remains in backing store
    
  }
  
  /* 
   * Verify an asserted checksum of a replica 
   * 
   * @param sourceNode - the source node of the object
   * @param replicaNode - the replica node of the object
   * @param pid - the identifier of the object
   * @param algorithm - the checksum algorithm
   */
  private boolean verifyChecksum(NodeReference sourceNode, NodeReference replicaNode,
    Identifier pid, String algorithm) 
    throws ServiceFailure, InvalidToken,
    NotAuthorized, NotFound, InvalidRequest, NotImplemented {
    
    boolean isValid = false;
    
    Checksum sourceChecksum = null;
    Checksum replicaChecksum = null;
    Session session = new Session();
    session.setSubject(null); // session will be overwritten by Session from SSL cert
    
    MNode replicaMN = D1Client.getMN(sourceNode);
    replicaChecksum = replicaMN.getChecksum(session, pid, algorithm);

    // Get the source node's checksum for the pid
    MNode sourceMN = D1Client.getMN(sourceNode);
    sourceChecksum = sourceMN.getChecksum(session, pid, algorithm);
    
    if ( sourceChecksum.equals(replicaChecksum) && sourceChecksum != null) {
      isValid = true;
      
    }
    
    return isValid;
    
  }
  
}
