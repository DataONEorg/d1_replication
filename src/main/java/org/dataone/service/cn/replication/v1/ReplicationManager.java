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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.base.RuntimeInterruptedException;

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
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Service;


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
public class ReplicationManager implements 
  EntryListener<Identifier, SystemMetadata>, ItemListener<MNReplicationTask> {

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
    
  /* The Hazelcast distributed task id generator namespace */
  private String taskIds;
  
  /* The Hazelcast distributed system metadata map */
  private IMap<NodeReference, Node> nodes;

  /* The Hazelcast distributed system metadata map */
  private IMap<Identifier, SystemMetadata> systemMetadata;
  
  /* The Hazelcast distributed replication tasks queue*/
  private IQueue<MNReplicationTask> replicationTasks;
  
  /**
   * Private Constructor - singleton pattern
   */
  public ReplicationManager() {
    
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
    this.taskIds = 
      Settings.getConfiguration().getString("dataone.hazelcast.tasksIdGenerator");
    
    // Become a Hazelcast cluster client using the replication structures
    String[] addresses = this.addressList.split(",");

    log.info("Becoming a DataONE Storage cluster hazelcast client where the group name " +
        "is " + this.groupName + " and the cluster member IP addresses are " +
        this.addressList + ".");
    
    this.hzClient = 
      HazelcastClient.newHazelcastClient(this.groupName, this.groupPassword, addresses);
    
    // Also become a Hazelcast processing cluster member
    log.info("Becoming a DataONE Process cluster hazelcast member with the default instance.");
    
    this.hzMember = Hazelcast.getDefaultInstance();

    this.nodes = this.hzMember.getMap(nodeMap);
    this.systemMetadata = this.hzClient.getMap(systemMetadataMap);
    this.replicationTasks = this.hzMember.getQueue(tasksQueue);
    this.taskIdGenerator = this.hzMember.getIdGenerator(taskIds);
    // monitor the replication structures
    
    log.info("Adding listeners to the " + this.systemMetadata.getName() +
        " map and the " + this.replicationTasks.getName() + " queue.");
    this.systemMetadata.addEntryListener(this, true);
    this.replicationTasks.addItemListener(this, true);
  }

  public void init() {
      log.info("initialization");
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

    boolean allowed;
    int taskCount = 0;
    int desiredReplicas;
    List<Replica> replicaList;             // the replica list for this pid
    Set<NodeReference> nodeList;           // the full nodes list
    List<NodeReference> potentialNodeList; // the MN subset of the nodes list
    Node targetNode = new Node();          // the target node for the replica
    Node originatingNode = new Node();     // the source node of the object
    
    try {
      
      // if replication isn't allowed, return
      allowed = isAllowed(pid);
      
      if ( !allowed ) {
        log.info("Replication is not allowed for the object identified by " +
            pid.getValue());
        return 0;
        
      }

      // get the system metadata for the pid
      SystemMetadata sysmeta = this.systemMetadata.get(pid);
      replicaList = sysmeta.getReplicaList();
      
      // List of Nodes for building MNReplicationTasks
      nodeList = (Set<NodeReference>) this.nodes.keySet();
      potentialNodeList = new ArrayList<NodeReference>(); // will be our short list
      
      // authoritative member node to replicate from
      try {
      originatingNode = this.nodes.get(sysmeta.getAuthoritativeMemberNode());
      } catch (NullPointerException npe) {
          throw new InvalidRequest("1080", "Object " + pid.getValue() + 
                  " has no authoritative Member Node in its SystemMetadata");
      }
      
      // build the potential list of target nodes
      for(NodeReference nodeReference : nodeList) {
        Node node = this.nodes.get(nodeReference);
          if ( node.getType() == NodeType.MN ) {
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

      // remove blocked nodes from the potential nodelist
      if ( !blockedList.isEmpty() ) {
        for (NodeReference blockedNode : blockedList) {
          if ( potentialNodeList.contains(blockedNode) ) {
            potentialNodeList.remove(blockedNode);
            
          }
        }
      }
      
      // prioritize preferred nodes in the potential node list
      if ( !preferredList.isEmpty() ) {
        
        // process preferred nodes backwards
        for (int i = preferredList.size() - 1; i >= 0; i--) {
          
          NodeReference preferredNode = preferredList.get(i);
          if ( potentialNodeList.contains( preferredNode) ) {
            potentialNodeList.remove(preferredNode); // remove for reordering
            potentialNodeList.add(0, preferredNode); // to the top of the list
             
          }
        }
      }
       
      
      // can't have more replicas than MNs
      if ( desiredReplicas > potentialNodeList.size() ) {
        desiredReplicas = potentialNodeList.size(); // yikes
                
      }
      
      boolean alreadyAdded = false;

      // for each node in the potential node list up to the desired replicas
      for(int j = 0; j < desiredReplicas; j++) {

        NodeReference potentialNode = potentialNodeList.get(j);
        // for each replica in the replica list
        for (Replica replica : replicaList) {
          
          // ensure that this node is not queued, requested, or completed
          NodeReference replicaNode = replica.getReplicaMemberNode();
          ReplicationStatus status = replica.getReplicationStatus();
          if (potentialNode.getValue().equals(replicaNode.getValue()) &&
             (status.equals(ReplicationStatus.QUEUED) ||
              status.equals(ReplicationStatus.REQUESTED) ||
              status.equals(ReplicationStatus.COMPLETED))){
            alreadyAdded = true;
            break;
            
          }
          
        }
        // if the node doesn't already exist as a pending task for this pid
        if ( !alreadyAdded ) {
          targetNode = this.nodes.get(potentialNode);
                
        }
          
        // update system metadata for the targetNode on this task
        SystemMetadata sysMeta = this.systemMetadata.get(pid);
        List<Replica> list = sysMeta.getReplicaList();
        boolean replicaAdded = false;
        
        for (Replica replica : list) {
          NodeReference replicaNode = replica.getReplicaMemberNode();
          if ( replicaNode.getValue().equals(targetNode) ) {
            replica.setReplicationStatus(ReplicationStatus.QUEUED);
            replicaAdded = true;
            break;
            
          }
        }
        
        String version = "";
        List<Service> origServices = originatingNode.getServices().getServiceList();
        for (Service service : origServices) {
            if(service.getName().equals("MNReplication")) {
                version = service.getVersion();
            }
        }
        if (version.equals("")) {
            throw new InvalidRequest("1080","MNReplication Service missing version");
        }

        boolean replicable = false;

        for (Service service : targetNode.getServices().getServiceList()) {
            if(service.getName().equals("MNReplication") &&
               service.getVersion().equals(version) &&
               service.getAvailable()) {
                replicable = true;
            }
        }

        // a replica doesn't exist. add it
        if ( !replicaAdded && replicable ) {
          Replica newReplica = new Replica();
          newReplica.setReplicaMemberNode(targetNode.getIdentifier());
          newReplica.setReplicationStatus(ReplicationStatus.QUEUED);
          sysMeta.addReplica(newReplica);

        }
        
        sysMeta.setDateSysMetadataModified(new Date());
        this.systemMetadata.put(pid, sysMeta);
        
        Long taskid = taskIdGenerator.newId();
        // add the task to the task list
        log.info("Adding a new MNreplicationTask to the queue where " +
          "pid = "                    + pid.getValue() + 
          ", originatingNode name = " + originatingNode.getName() +
          ", targetNode name = "      + targetNode.getName());
        
        MNReplicationTask task = new MNReplicationTask(
                            taskid.toString(),
                            pid,
                            originatingNode,
                            targetNode,
                            Permission.REPLICATE);
        this.replicationTasks.add(task);
        taskCount++;

      }

          

      
    } catch (InvalidRequest ir) {

        ir.printStackTrace();

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // return the number of replication tasks queued
    return taskCount;
    
  }
  
  /**
   * Regular replication sweep over all of the objects on MNs
   *
  public void replicationSweep() {

      nodeList = (Set<NodeReference>) this.nodes.keySet();

      for (NodeReference nodeRef : nodeList) {
          // 
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
      task = this.replicationTasks.poll(3, TimeUnit.SECONDS);
      
      if ( task != null ) {
        log.info("Submitting replication task id " + task.getTaskid() +
                 " for execution with object identifier: " + task.getPid().getValue());
        
        // TODO: handle the case when a CN drops and the MNReplicationTask.call()
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
   * Implement the EntryListener interface, responding to entries being added to
   * the hzSystemMetadata map.
   * 
   * @param event - the entry event being added to the map
   */
  public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
    
    try {
      
      log.info("Received entry added event on the " +
        this.systemMetadata.getName() + " map. Evaluating it for MN replication tasks.");
      
      // lock the pid and handle the event. If it's already pending, do nothing      
      this.systemMetadata.lock(event.getKey());
      
      // also need to check the current replicationTasks
      boolean no_task_with_pid = true;

      // check if replication is allowed
      boolean allowed = isAllowed(event.getKey());
      
      if ( allowed ) {
        // check if there are pending tasks
        boolean is_pending = isPending(event.getKey());
        
        // for each MNReplicationTask
        for (MNReplicationTask task : replicationTasks) {
          // if the task's pid is equal to the event's pid
          if (task.getPid().getValue().equals(event.getKey().getValue())) {
            no_task_with_pid = false;
            break;

          }
        }
        // if the pid is locked and not taken by a pending task
        if (!is_pending && no_task_with_pid) {
          log.info("Calling createAndQueueTasks for identifier: " +
              event.getKey().getValue());
          this.createAndQueueTasks(event.getKey());
          this.systemMetadata.unlock(event.getKey());

        }
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
   * Check if replication is allowed for the given pid
   * @param pid - the identifier of the object to check
   * @return
   */
  public boolean isAllowed(Identifier pid) {
    boolean isAllowed = false;
    SystemMetadata sysmeta = this.systemMetadata.get(pid);
    ReplicationPolicy policy = sysmeta.getReplicationPolicy();
    
    return policy.getReplicationAllowed().booleanValue();
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
      
      log.info("Received entry updated event on the " +
        this.systemMetadata.getName() + " map. Evaluating it for MN replication tasks.");

      // lock the pid and handle the event. If it's already pending, do nothing
      this.systemMetadata.lock(event.getKey());

      // also need to check the current replicationTasks
      boolean no_task_with_pid = true;

      // check if replication is allowed
      boolean allowed = isAllowed(event.getKey());

      if ( allowed ) {
        // check if there are pending tasks
        boolean is_pending = isPending(event.getKey());
        
        // for each MNReplicationTask
        for (MNReplicationTask task : replicationTasks) {
          // if the task's pid is equal to the event's pid
          if (task.getPid().getValue().equals(event.getKey().getValue())) {
            no_task_with_pid = false;
            break;
          }
        }
        // if the pid is locked and not taken by a pending task
        if (!is_pending && no_task_with_pid) {
          log.info("Calling createAndQueueTasks for identifier: " +
              event.getKey().getValue());
          this.createAndQueueTasks(event.getKey());
          this.systemMetadata.unlock(event.getKey());
        }
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
  
}
