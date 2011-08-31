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
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Instance; 
import com.hazelcast.core.InstanceEvent; 
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.ItemListener;
import com.hazelcast.query.SqlPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.ObjectFormatCache;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNReplication;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;

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
public class ReplicationService implements CNReplication, InstanceListener,
  ItemListener<ReplicationTask> {

	/* Get a Log instance */
	public static Log log = LogFactory.getLog(ReplicationService.class);
	
	/* The instance of the replication service */
	private ReplicationService instance;
	
	/* The name of the replication tasks queue */
	private String tasksQueue = 
		Settings.getConfiguration().getString("replication.hazelcast.queuedTasks");
	
	/* The name of the pending replication tasks map */
	private String pendingTasksQueue = 
		Settings.getConfiguration().getString("replication.hazelcast.pendingTasks");
	
	/* The Hazelcast distributed replication tasks queue*/
	private IQueue<ReplicationTask> replicationTasks;
	
	/* The Hazelcast distributed pending replication tasks map*/
	private Map<String, ReplicationTask> pendingReplicationTasks;
	
	/**
	 * Private Constructor - singleton pattern
	 */
	private ReplicationService() {
		super();
		
		// Become a Hazelcast cluster node using the replication structures
		replicationTasks = Hazelcast.getQueue("taskQueue");
		pendingReplicationTasks = Hazelcast.getMap(pendingTasksQueue);
		
		// monitor the replication tasks queue
    replicationTasks.addItemListener(this, true);
	}

	/**
	 * Get an instance of the ReplicationService, or create one if it doesn't exist
	 * 
	 * @return instance - the instance of the ReplicationService
	 */
	public synchronized ReplicationService getInstance() {
		
  	if ( instance == null ) {
  		instance = new ReplicationService();
  		
  	}
  	
  	return instance;
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
	public boolean isReplicationAuthorized(Session originatingNodeSession,
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
		
		IMap pendingReplicationTasks = Hazelcast.getMap("hzPendingReplicationTasks");
		Set<ReplicationTask> tasks = 
			(Set<ReplicationTask>) pendingReplicationTasks.values(new SqlPredicate(query));
		
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
	    	
		    ExecutorService executorService = Hazelcast.getExecutorService();
		    Future<String> replicationTask = executorService.submit(task);
		    
		    // check for completion
		    while ( !replicationTask.isDone() ) {
		    	
		    	if ( replicationTask.isCancelled() ) {
		    		log.info("Replication task id " + task.getTaskid() + 
		    				     " was cancelled.");
		    	}
		    	
		    }
		    
    		log.info("Replication task id " + task.getTaskid() + " completed.");
        
    		//TODO: lock pid, update sysmeta to set ReplicationStatus.COMPLETE
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
	
	
}
