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
 * 
 * $Id: $
 */

package org.dataone.service.cn.replication.v1;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;
import org.dataone.cn.hazelcast.HazelcastClientInstance;

/**
 * A single replication task to be queued and executed by the Replication Service.
 * The task is built from information found in the Replication Policy of an
 * object's system metadata and from information about a target node's 
 * capabilities.
 * 
 * @author cjones
 *
 */
public class MNReplicationTask implements Serializable, Callable<String> {

  /* Get a Log instance */
  public static Log log = LogFactory.getLog(MNReplicationTask.class);
  
  /* The identifier of this task */
  private String taskid;
  
  /* The identifier of the system metadata map event that precipitated this task */
  private String eventid;
  
  /* The identifier of the object to replicate */
  private String pid;
  
  /* The target Node object */
  private Node targetNode;
  
  /* The originating Node object */
  private Node originatingNode;

  /* The subject of the target node, extracted from the Node object */
  private String targetNodeSubject;
  
  /* The subject of the originating node, extracted from the Node object */
  private String originatingNodeSubject;
  
  /* The permission to be executed (in this case, always 'replicate') */
  String permission;

  /**
   * Constructor - create an empty replication task instance
   */
  public MNReplicationTask() {
  }

  /**
   * Constructor - create a replication task instance
   * 
   * @param taskid
   * @param pid
   * @param targetNode
   */
  public MNReplicationTask(String taskid, Identifier pid, 
    Node originatingNode, Node targetNode,
    Permission replicatePermission) {
    
    this.taskid = taskid;
    this.pid = pid.getValue();
    this.originatingNode = originatingNode;
    this.targetNode = targetNode;
    this.originatingNodeSubject = originatingNode.getSubject(0).getValue();
    this.targetNodeSubject = targetNode.getSubject(0).getValue();
    this.permission = replicatePermission.name();
    
  }

  /**
   * Get the task identifier for this task
   * @return the taskid
   */
  public String getTaskid() {
    return taskid;
  }

  /**
   * Set the task identifier for this task
   * @param taskid the taskid to set
   */
  public void setTaskid(String taskid) {
    this.taskid = taskid;
  }

  /**
   * Get the object identifier to be replicated
   * @return the pid
   */
  public Identifier getPid() {
    Identifier identifier = new Identifier();
    identifier.setValue(pid);
    return identifier;
  }

  /**
   * Set the object identifier to be replicated
   * @param pid the pid to set
   */
  public void setPid(Identifier pid) {
    this.pid = pid.getValue();
  }

  /**
   * Get the event identifier 
   * @return the eventid
   */
  public String getEventid() {
    return eventid;
  }

  /**
   * Set the event identifier 
   * @param eventid the eventid to set
   */
  public void setEventid(String eventid) {
    this.eventid = eventid;
  }

  /**
   * Get the target node
   * @return the targetNode
   */
  public Node getTargetNode() {
    return targetNode;
  }

  /**
   * Set the target node
   * @param targetNode the targetNode to set
   */
  public void setTargetNode(Node targetNode) {
    this.targetNode = targetNode;
  }

  /**
   * Get the originating node
   * @return the originatingNode
   */
  public Node getOriginatingNode() {
    return originatingNode;
  }

  /**
   * Set the originating node
   * @param originatingNode the originatingNode to set
   */
  public void setOriginatingNode(Node originatingNode) {
    this.originatingNode = originatingNode;
  }

  /**
   * For the given Replication task, return the Subject listed in the target
   * node.  Usually used in authorizing a replication event.
   * 
   * @return subject - the subject listed in the target Node object as a string
   */
  public String getTargetNodeSubject() {
    
    return this.targetNodeSubject;
    
  }
  
  /**
   * Set the target node subject identifying the node
   * @param subject the targetNode subject
   */
  public void setTargetNodeSubject(String subject) {
    this.targetNodeSubject = subject;
  }
  
  /**
   * For the given Replication task, return the Subject listed in the target
   * node.  Usually used in authorizing a replication event.
   * 
   * @return subject - the subject listed in the target Node object as a string
   */
  public String getOriginatingNodeSubject() {
    
    return this.originatingNodeSubject;
    
  }
  
  /**
   * Set the target node subject identifying the node
   * @param subject the targetNode subject
   */
  public void setOriginatingNodeSubject(String subject) {
    this.originatingNodeSubject = subject;
  }
  
  /**
   * Get the permission being allowed for this task
   * 
   * @return subject - the subject listed in the target Node object
   */
  public String getPermission() {
    return this.permission;
    
  }
  
  /**
   * Set the permission being allowed for this task
   * @param subject the targetNode subject
   */
  public void setPermission(Permission permission) {
    this.permission = permission.name();
    
  }
 
  /**
   * Implement the Callable interface, providing code that initiates replication.
   * 
   * @return pid - the identifier of the replicated object upon success
   */
  public String call() throws IllegalStateException {

  log.info("MNReplicationTask.call() called for identifier " + this.pid);
  
	MNode targetMN = null;
  
	// Get an target MNode reference to communicate with
	try {
	  log.info("Getting the MNode reference for " + targetNode.getIdentifier().getValue());
	  targetMN = D1Client.getMN(targetNode.getIdentifier());
	  
    } catch (ServiceFailure e) {
      log.info("Failed to get the target MNode reference for " + 
  	    targetNode.getIdentifier().getValue() + 
  	    " while executing MNreplicationTask id " +
  	    this.taskid);
  	
  }
	
	// Get the D1 Hazelcast configuration parameters
	 String hzSystemMetadata =
	 	Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");

	// String groupName =
	//	Settings.getConfiguration().getString("dataone.hazelcast.groupName");

	// String groupPassword =
	// 	Settings.getConfiguration().getString("dataone.hazelcast.password");
	
	// String addressList =
	// 	Settings.getConfiguration().getString("dataone.hazelcast.clusterInstances");
	// String[] addresses = addressList.split(",");
	
	// log.info("Becoming a DataONE storage cluster client with group name: " +
	//       groupName + ", password: " + groupPassword + ", cluster instances: " +
	//        addresses);
	
	// get the system metadata for the pid
	// HazelcastClient hzClient =
	//	HazelcastClient.newHazelcastClient(groupName, groupPassword, addresses);
	HazelcastClient hzClient = HazelcastClientInstance.getHazelcastClient();
	IMap<Identifier, SystemMetadata> sysMetaMap = hzClient.getMap(hzSystemMetadata);
	
	// Initiate the MN to MN replication for this task
	try {
	    log.info("Getting a lock on identifier " + this.pid + " for task id " +
	            this.taskid);
	    sysMetaMap.lock(getPid());
	    SystemMetadata sysmeta = sysMetaMap.get(getPid());
	    log.info("Lock acquired for identifier " + this.pid);
	    
      log.info("Evaluating replica list for identifer " + this.pid);
      List<Replica> replicaList = sysmeta.getReplicaList();
      boolean replicaEntryExists = false;
      
      // set the replica status for the correct replica
      for (Replica replica : replicaList ) {
        
        if (replica.getReplicaMemberNode() == this.targetNode.getIdentifier()) {
          replica.setReplicationStatus(ReplicationStatus.REQUESTED);
          replicaEntryExists = true;
          log.info("Setting the replication status for identifier " + this.pid + 
                  " and replica node " + replica.getReplicaMemberNode().getValue() + 
                  " to " + ReplicationStatus.REQUESTED);
          break;

        }
      
      }
    
    // no replica exists yet, make one
    if ( !replicaEntryExists || replicaList == null || replicaList.isEmpty()) {
      Replica newReplica = new Replica();
      newReplica.setReplicaMemberNode(this.targetNode.getIdentifier());
      newReplica.setReplicationStatus(ReplicationStatus.REQUESTED);
      replicaList.add(newReplica);
      log.info("Setting the replication status for identifier " + this.pid + 
              " and replica node " + newReplica.getReplicaMemberNode().getValue() + 
              " to " + ReplicationStatus.REQUESTED);

    }

    // update the system metadata object
	  sysmeta.setDateSysMetadataModified(new Date());
	  sysmeta.setReplicaList(replicaList);
	  
	  // set up the certificate location
	  String clientCertificateLocation = 
		  Settings.getConfiguration().getString("D1Client.certificate.directory") +
		  "/" +Settings.getConfiguration().getString("cn.nodeId");
	  CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
	  log.info("MNReplicationTask task id " + this.taskid + "is using an X509 certificate " +
	    "from " + clientCertificateLocation);

	  // session is null - certificate is used
	  Session session = null;
	  
	  // call for the replication
	  log.info("Calling MNreplication.replicate() at targetNode id " + targetMN.getNodeId());
	  targetMN.replicate(session, sysmeta, this.originatingNode.getIdentifier());
	  
	  // update the system metadata map
	  sysMetaMap.put(getPid(), sysmeta);
	  
	  log.info("Updated system metadata for identifier " + this.pid + "during " +
	          " MNreplicationTask id " + this.taskid);
      sysMetaMap.unlock(getPid());

	} catch (NotImplemented e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
    
	} catch (ServiceFailure e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
    
	} catch (NotAuthorized e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
    
	} catch (InvalidRequest e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
    
	} catch (InsufficientResources e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
    
	} catch (UnsupportedType e) {
	  // TODO Auto-generated catch block
	  e.printStackTrace();
  
    } finally {
      sysMetaMap.unlock(getPid());
    
    }
	    
	
    return this.pid;
  }

}
