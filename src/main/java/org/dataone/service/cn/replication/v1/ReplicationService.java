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

import com.hazelcast.core.Hazelcast; 
import com.hazelcast.core.Instance; 
import com.hazelcast.core.InstanceEvent; 
import com.hazelcast.core.InstanceListener;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;

/**
 * A DataONE Coordinating Node implementation of the CNReplication API which
 * manages replication queues and executes replication tasks.
 * 
 * @author cjones
 *
 */
public class ReplicationService implements CNReplication, InstanceListener {

	/* Get a Log instance */
	public static Log log = LogFactory.getLog(ReplicationService.class);
	
	/**
	 * Constructor
	 */
	public ReplicationService() {
		
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
    ReplicationStatus replicationStatus) 
	  throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
	  InvalidRequest, NotFound {

    throw new NotImplemented("", "");
    
  }

	/**
	 * Create a list of replication tasks given the identifier of an object
	 * by evaluating its system metadata and the capabilities of the target
	 * replication nodes
	 * 
	 * @param pid - the identifier of the object to be replicated
	 * @return taskList - the list of replication tasks
	 * 
	 * @throws ServiceFailure
	 * @throws NotImplemented
	 * @throws InvalidToken
	 * @throws NotAuthorized
	 * @throws InvalidRequest
	 * @throws NotFound
	 */
	public List<ReplicationTask> createReplicationTaskList(Identifier pid) 
	throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
  InvalidRequest, NotFound {
		
		List<ReplicationTask> taskList = new ArrayList<ReplicationTask>();

		throw new NotImplemented("", "");

		// get the system metadata for the pid
		
		// parse the sysmeta.ReplicationPolicy
		
		// prioritize replication nodes based on the policy and node capabilities
		
		// create the ReplicationTask
		
		// add the task to the task list
				
		//return taskList;
	}
	
	/**
	 * Process a replication task by initiating replication on the target node
	 * 
	 * @param replicationTask - the replication task to be executed
	 * @return boolean - true if the task is intiated without exception
	 * 
	 * @throws ServiceFailure
	 * @throws NotImplemented
	 * @throws InvalidToken
	 * @throws NotAuthorized
	 * @throws InvalidRequest
	 * @throws NotFound
	 */
	public boolean handleReplicationTask(ReplicationTask replicationTask) 
	  throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
    InvalidRequest, NotFound {
		
		throw new NotImplemented("", "");
		
		// get a Subject token to identify this CN
		
		// call replicate() on the target MN using ReplicationTask data, and
		// libclient's REST call methods
		
		// handle exceptions from the REST call
		
		
		//return false;
		
	}

	/**
	 * Called when this Hazelcast instance listener is created
	 * 
	 * @param event - the instance event that occurred
	 */
	public void instanceCreated(InstanceEvent event) {
    
		Instance instance = event.getInstance();
		log.info("Created Hazelcast instance: " + instance.getInstanceType() +
		  ", " + instance.getId());
  }

	/**
	 * Called when this Hazelcast instance listener is destroyed
	 * 
	 * @param event - the instance event that occurred
	 */
	public void instanceDestroyed(InstanceEvent event) {
		
		Instance instance = event.getInstance();
		log.info("Destroyed Hazelcast instance: " + instance.getInstanceType() +
		  ", " + instance.getId());
	  
  }
	
	
}
