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
 * $Id$
 */
package org.dataone.service.cn.replication.v1;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import org.dataone.service.types.v1.NodeReference;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;


/**
 * A single replication task to be queued and executed by the Replication Service.
 * The task is built from information found in the Replication Policy of an
 * object's system metadata and from information about a target node's 
 * capabilities.
 * 
 * @author cjones
 *
 */
public class MNReplicationTask
    implements Serializable, Callable<String> {

    /* Get a Log instance */
    public static Log log = LogFactory.getLog(MNReplicationTask.class);
    
    /* The identifier of this task */
    private String taskid;
    
    /* The identifier of the system metadata map event that precipitated this task */
    private String eventid;
    
    /* The identifier of the object to replicate */
    private Identifier pid;
    
    /* The target Node object */
    private NodeReference targetNode;
    
    /* The originating Node object */
    private NodeReference originatingNode;
    
    /* The subject of the target node, extracted from the Node object */
    private String targetNodeSubject;
    
    /* The subject of the originating node, extracted from the Node object */
    private String originatingNodeSubject;
    
    /* A client reference to the target node */
    private MNode targetMN;
    
    /* A client reference to the coordinating node */
    private CNode cn;
    
    /* The number of times the task has been retried */
    private int retryCount;

    /* The name of the hzReplicationTasks property */
    private String tasksQueue;

    /* The instance of the Hazelcast processing cluster member */
    private HazelcastInstance hzMember;

    /* The Hazelcast distributed replication tasks queue */
    private IQueue<MNReplicationTask> replicationTasks;

    private String cnRouterBaseURL;

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
            NodeReference originatingNode, NodeReference targetNode) {
        this.taskid = taskid;
        // get a reference to the hzReplicationTasks queue
        this.tasksQueue = 
            Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
        this.hzMember = Hazelcast.getDefaultInstance();
        this.replicationTasks = this.hzMember.getQueue(this.tasksQueue);

        this.pid = pid;
        this.originatingNode = originatingNode;
        this.targetNode = targetNode;
        // set up the certificate location
        String clientCertificateLocation =
                Settings.getConfiguration().getString("D1Client.certificate.directory")
                + File.separator + Settings.getConfiguration().getString("D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        log.info("MNReplicationTask task id " + this.taskid + " is using an X509 certificate "
                + "from " + clientCertificateLocation + " for identifier " + this.pid.getValue());
        this.cnRouterBaseURL = "https://" +
            Settings.getConfiguration().getString("cn.router.hostname") + "/cn";
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
        return this.pid;
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
    public NodeReference getTargetNode() {
        return targetNode;
    }

    /**
     * Set the target node
     * @param targetNode the targetNode to set
     */
    public void setTargetNode(NodeReference targetNode) {
        this.targetNode = targetNode;
    }

    /**
     * Get the originating node
     * @return the originatingNode
     */
    public NodeReference getOriginatingNode() {
        return originatingNode;
    }

    /**
     * Set the originating node
     * @param originatingNode the originatingNode to set
     */
    public void setOriginatingNode(NodeReference originatingNode) {
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
     * Implement the Callable interface, providing code that initiates replication.
     *
     * @return pid - the identifier of the replicated object upon success
     */
    public String call() {

        log.info("Replication attempt # " + getRetryCount() + 
            " for replication task " + getTaskid() + " for identifier " + 
            getPid().getValue() + " on node " + getTargetNode().getValue());
        
        SystemMetadata sysmeta = null;
        
        // a flag showing an already existing replica on the target MN
        boolean exists = false;
        
        // a flag for success on setting replication status
        boolean success = false;
        
        // session is null - certificate is used
        Session session = null;
                
        // Get an CNode reference to communicate with
        try {
            this.cn = D1Client.getCN();
        
        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the CN " +
                "during replication task id " + getTaskid() + ", identifier " +
                getPid().getValue() + ", target node " + getTargetNode().getValue());
            // try again, then fail
            try {
                Thread.sleep(5000L);
                this.cn = D1Client.getCN();
            
            } catch (ServiceFailure e1) {
                log.warn("Second ServiceFailure while getting a reference to the CN " +
                    "during replication task id " + getTaskid() + ", identifier " +
                    getPid().getValue() + ", target node " + getTargetNode().getValue());
                e1.printStackTrace();
                this.cn = null;
                success = false;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the CN " +
                    "during replication task id " + getTaskid() + ", identifier " +
                    getPid().getValue() + ", target node " + getTargetNode().getValue());
                ie.printStackTrace();
                this.cn = null;
                success = false;

            }
        }

        // Get an target MNode reference to communicate with
        try {
            this.targetMN = D1Client.getMN(this.targetNode);
       
        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the MN " +
                    "during replication task id " + getTaskid() + ", identifier " +
                    getPid().getValue() + ", target node " + getTargetNode().getValue());
            
            try {
                // wait 5 seconds and try again, else fail
                Thread.sleep(5000L);
                this.targetMN = D1Client.getMN(this.targetNode);
            
            } catch (ServiceFailure e1) {

                log.error("There was a problem calling replicate() on " +
                        getTargetNode().getValue() + " for identifier " + 
                        this.pid.getValue() + " during " + 
                        " task id " + getTaskid());
                e1.printStackTrace();
                success = false;
                                    
            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the MN " +
                        "during replication task id " + getTaskid() + ", identifier " +
                        getPid().getValue() + ", target node " + getTargetNode().getValue());
                ie.printStackTrace();
                success = false;

            }
        }

        try {

            if (this.cn != null ) {
                // get the most recent system metadata for the pid
                sysmeta = cn.getSystemMetadata(session, pid);
                // call for the replication
                
                // check if the object exists on the target MN already
                try {
                    Checksum checksum = this.targetMN.getChecksum(getPid(), "SHA-1");
                    exists = checksum.equals(sysmeta.getChecksum());
                    
                } catch (NotFound nfe) {
                    success = this.targetMN.replicate(session, sysmeta, this.originatingNode);
                    log.info("Task id " + this.getTaskid() + " called replicate() at targetNode " + 
                            this.targetNode.getValue() + ", identifier " + this.pid.getValue() +
                            ". Success: " + success);
                }
               
            } else {
                log.error("Can't get system metadata: CNode object is null for " +
                    " task id " + getTaskid() + ", identifier " + getPid().getValue() +
                    ", target node " + getTargetNode().getValue());
                success = false;
            }
                        
        } catch (BaseException e) {
                       
            try {
                log.info("The call to MN.replicate() failed for " + pid.getValue() +
                    " on " + this.targetNode.getValue() + ". Trying again in 5 seconds.");
                this.retryCount++;
                Thread.sleep(5000L);
                // get the most recent system metadata for the pid
                if (this.cn != null ) {
                    try {
                        Checksum checksum = this.targetMN.getChecksum(getPid(), "SHA-1");
                        exists = checksum.equals(sysmeta.getChecksum());
                        
                    } catch (NotFound nf) {
                        sysmeta = cn.getSystemMetadata(session, pid);
                        success = targetMN.replicate(session, sysmeta, this.originatingNode);
                        log.info("Task id " + this.getTaskid() + " called replicate() at targetNode " + 
                                this.targetNode.getValue() + ", identifier " + this.pid.getValue() +
                                ". Success: " + success);
                    }
                    
                } else {
                    log.error("Can't get system metadata: CNode object is null for " +
                        " task id " + getTaskid() + ", identifier " + getPid().getValue() +
                        ", target node " + getTargetNode().getValue());
                    success = false;
                }
                               
            } catch (BaseException e1) {
                
                // still couldn't call replicate() successfully. fail.
                log.error("There was a second problem calling replicate() on " +
                        getTargetNode().getValue() + " for identifier " + 
                        getPid().getValue() + " during " + 
                        " task id " + getTaskid());
                e1.printStackTrace();
                success = false;
                                                
            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while calling replicate() " +
                    "during replication task id " + getTaskid() + ", identifier " +
                    getPid().getValue() + ", target node " + getTargetNode().getValue());
                ie.printStackTrace();
                success = false;

            }
            
        } catch (Exception e) {
            log.error("Unknown exception during replication task id " +
                getTaskid() + ", identifier " + getPid().getValue() + 
                ", target node " + getTargetNode().getValue() + ". Error message: " +
                e.getMessage());
            success = false;
            
        }
        
        // update the replication status
        ReplicationStatus status = null;
        if ( success ) {
            status = ReplicationStatus.REQUESTED;
            
        } else {
            // re-queue the task up to 10 times, then fail
            if ( this.retryCount < 10) {
                status = ReplicationStatus.QUEUED;
                this.replicationTasks.add(this);
                log.debug("Task " + this.getTaskid() + " was re-queued " +
                    "for identifier " + this.pid.getValue() + " on node " +
                    this.targetNode.getValue());
                
            } else {
                status = ReplicationStatus.FAILED;
                
            }
        }
        
        // for replicas that already exist, just update the system metadata
        if ( exists ) {
            status = ReplicationStatus.COMPLETED;
            
        }

        try {
            if (this.cn != null) {
                
                cn.setReplicationStatus(session, pid, targetNode, status, null);
                log.info( "Task " + this.getTaskid() + " updated replica status for identifier " + 
                    this.pid.getValue() + " on node " + 
                    this.targetNode.getValue() + " to " + status.toString());
            } else {
                log.error( "Task " + this.getTaskid() + 
                    " can't update replica status for identifier " + 
                    this.pid.getValue() + " on node " + 
                    this.targetNode.getValue() + " to " + status.toString() +
                    ". CNode reference is null, trying the router address");
                // try setting the status against the router address
                CNode routerCN = new CNode(this.cnRouterBaseURL);
                routerCN.setReplicationStatus(session, pid, targetNode, status, null);
            }
            
        } catch (BaseException e1) {
            log.error("There was a problem setting the replication status to " +
                    status.toString() + "  for identifier " + 
                    this.pid.getValue() + " during " + 
                    " MNReplicationTask id " + this.taskid);
       }               
        log.trace("METRICS:\tREPLICATION:\tEND QUEUE:\tPID:\t" + pid.getValue() + 
                "\tNODE:\t" + targetNode.getValue() + 
                "\tSIZE:\t" + sysmeta.getSize().intValue());
        log.trace("METRICS:\tREPLICATION:\t" + status.toString() + 
                ":\tPID:\t" + pid.getValue() + 
                "\tNODE:\t" + targetNode.getValue() + 
                "\tSIZE:\t" + sysmeta.getSize().intValue());

       return this.pid.getValue();
    }

    /**
     * Implements the Runnable interface, but the task is actually called via
     * the Callable interface.  This is needed as a placeholder to handle
     * rejected tasks via the RejectedReplicationTaskHandler class.
     */
    public void run() {
        log.debug("MNReplicationTask.run() called.");       
        
    }

    /**
     * Set the number of retries for this particular replication task
     * 
     * @param retryCount
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Get the number of retries for this particular replication task;
     * 
     * @return retryCount  the number of retries for this replication task
     */
    public int getRetryCount() {
        return this.retryCount;
    }
}
