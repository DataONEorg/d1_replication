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

import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;

/**
 * A single replication task to be queued and executed by the Replication Service.
 * The task is built from information found in the Replication Policy of an
 * object's system metadata and from information about a target node's 
 * capabilities.
 * 
 * @author cjones
 *
 */
public class ReplicationTask {

	/* The identifier of this task */
	private String taskid;
	
	/* The identifier of the object to replicate */
	private Identifier pid;
	
	/* The node to replicate data to */
	private NodeReference targetNode;

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
  	return pid;
  }

	/**
	 * Set the object identifier to be replicated
   * @param pid the pid to set
   */
  public void setPid(Identifier pid) {
  	this.pid = pid;
  }

	/**
	 * Get the target node reference to replicate the object to
   * @return the targetNode
   */
  public NodeReference getTargetNode() {
  	return targetNode;
  }

	/**
	 * Set the target node reference to replicate the object to
   * @param targetNode the targetNode to set
   */
  public void setTargetNode(NodeReference targetNode) {
  	this.targetNode = targetNode;
  }
	
	
}
