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

import java.io.Serializable;

import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;

/**
 * A single replication task to be queued and executed by the Replication Service.
 * The task is built from information found in the Replication Policy of an
 * object's system metadata and from information about a target node's 
 * capabilities.
 * 
 * @author cjones
 *
 */
public class ReplicationTask implements Serializable {

	/* The identifier of this task */
	private String taskid;
	
	/* The identifier of the object to replicate */
	private String pid;
	
	/* The subject of the target node, extracted from the Node object */
	private String targetNodeSubject;
	
	/* The subject of the originating node, extracted from the Node object */
	private String originatingNodeSubject;
	
	/* The permission to be executed (in this case, always 'replicate') */
	String permission;

	/**
	 * Constructor - create an empty replication task instance
   */
  public ReplicationTask() {
  }

	/**
	 * Constructor - create a replication task instance
	 * 
   * @param taskid
   * @param pid
   * @param targetNode
   */
  public ReplicationTask(String taskid, Identifier pid, 
  	String  originatingNodeSubject, String targetNodeSubject,
  	Permission replicatePermission) {
	  
  	this.taskid = taskid;
	  this.pid = pid.getValue();
	  this.originatingNodeSubject = originatingNodeSubject;
	  this.targetNodeSubject = targetNodeSubject;
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

}
