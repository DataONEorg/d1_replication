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
	
	/* The node to replicate data to. This contains the Subject used for authorization */
	private Node targetNode;

	/* The subject of the target node, extracted from the Node object */
	private String subject;

	/**
	 * Constructor - create an empty replication task instance
   */
  public ReplicationTask() {
	  this.taskid = taskid;
	  this.pid = pid;
	  this.targetNode = targetNode;
  }

	/**
	 * Constructor - create a replication task instance
	 * 
   * @param taskid
   * @param pid
   * @param targetNode
   */
  public ReplicationTask(String taskid, Identifier pid, Node targetNode) {
	  
  	this.taskid = taskid;
	  this.pid = pid.getValue();
	  this.targetNode = targetNode;
	  // TODO: uncomment when Types.Node is modified to have Node.Subject
	  //this.subject = targetNode.getSubject().getValue();
	  
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
	 * Get the target node reference to replicate the object to
   * @return the targetNode
   */
  public Node getTargetNode() {
  	return targetNode;
  }

	/**
	 * Set the target node reference to replicate the object to
   * @param targetNode the targetNode to set
   */
  public void setTargetNode(Node targetNode) {
  	this.targetNode = targetNode;
  }
	
  /**
   * For the given Replication task, return the Subject listed in the target
   * node.  Usually used in authorizing a replication event.
   * 
   * @return subject - the Subject listed in the target Node object
   */
	public Subject getNodeSubject() {
		
		Subject subject = null;
		// TODO: uncomment when Types.Node is updated with Node.Subject
		//subject = targetNode.getSubject();
		return subject;
		
	}
	
	/**
	 * Set the target node subject identifying the node
   * @param subject the targetNode subject
   */
  public void setNodeSubject(String subject) {
  	this.subject = subject;
  }
	

}
