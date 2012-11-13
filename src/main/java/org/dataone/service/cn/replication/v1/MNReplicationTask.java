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

import java.io.File;
import java.io.Serializable;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

/**
 * A single replication task to be queued and executed by the Replication
 * Service. The task is built from information found in the Replication Policy
 * of an object's system metadata and from information about a target node's
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

    /*
     * The identifier of the system metadata map event that precipitated this
     * task
     */
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

    private String cnRouterHostname;

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
    public MNReplicationTask(String taskid, Identifier pid, NodeReference originatingNode,
            NodeReference targetNode) {
        this.taskid = taskid;
        this.hzMember = Hazelcast.getDefaultInstance();

        this.pid = pid;
        this.originatingNode = originatingNode;
        this.targetNode = targetNode;
        // set up the certificate location
        String clientCertificateLocation = Settings.getConfiguration().getString(
                "D1Client.certificate.directory")
                + File.separator
                + Settings.getConfiguration().getString("D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        X509Certificate certificate = CertificateManager.getInstance().loadCertificate();
        String X500SubjectStr = CertificateManager.getInstance().getSubjectDN(certificate);
        log.info("MNReplicationTask task id " + this.taskid
                + " is using an X509 certificate with subject " + X500SubjectStr + " from "
                + clientCertificateLocation + " for identifier " + this.pid.getValue());
        this.cnRouterHostname = "https://"
                + Settings.getConfiguration().getString("cn.router.hostname") + "/cn";
    }

    /**
     * Get the task identifier for this task
     * 
     * @return the taskid
     */
    public String getTaskid() {
        return taskid;
    }

    /**
     * Set the task identifier for this task
     * 
     * @param taskid
     *            the taskid to set
     */
    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }

    /**
     * Get the object identifier to be replicated
     * 
     * @return the pid
     */
    public Identifier getPid() {
        return this.pid;
    }

    /**
     * Get the event identifier
     * 
     * @return the eventid
     */
    public String getEventid() {
        return eventid;
    }

    /**
     * Set the event identifier
     * 
     * @param eventid
     *            the eventid to set
     */
    public void setEventid(String eventid) {
        this.eventid = eventid;
    }

    /**
     * Get the target node
     * 
     * @return the targetNode
     */
    public NodeReference getTargetNode() {
        return targetNode;
    }

    /**
     * Set the target node
     * 
     * @param targetNode
     *            the targetNode to set
     */
    public void setTargetNode(NodeReference targetNode) {
        this.targetNode = targetNode;
    }

    /**
     * Get the originating node
     * 
     * @return the originatingNode
     */
    public NodeReference getOriginatingNode() {
        return originatingNode;
    }

    /**
     * Set the originating node
     * 
     * @param originatingNode
     *            the originatingNode to set
     */
    public void setOriginatingNode(NodeReference originatingNode) {
        this.originatingNode = originatingNode;
    }

    /**
     * For the given Replication task, return the Subject listed in the target
     * node. Usually used in authorizing a replication event.
     * 
     * @return subject - the subject listed in the target Node object as a
     *         string
     */
    public String getTargetNodeSubject() {

        return this.targetNodeSubject;

    }

    /**
     * Set the target node subject identifying the node
     * 
     * @param subject
     *            the targetNode subject
     */
    public void setTargetNodeSubject(String subject) {
        this.targetNodeSubject = subject;
    }

    /**
     * For the given Replication task, return the Subject listed in the target
     * node. Usually used in authorizing a replication event.
     * 
     * @return subject - the subject listed in the target Node object as a
     *         string
     */
    public String getOriginatingNodeSubject() {

        return this.originatingNodeSubject;

    }

    /**
     * Set the target node subject identifying the node
     * 
     * @param subject
     *            the targetNode subject
     */
    public void setOriginatingNodeSubject(String subject) {
        this.originatingNodeSubject = subject;
    }

    /**
     * Implement the Callable interface, providing code that initiates
     * replication.
     * 
     * @return pid - the identifier of the replicated object upon success
     */
    public String call() {

        log.info("Replication attempt # " + (getRetryCount() + 1) + " for replication task "
                + getTaskid() + " for identifier " + getPid().getValue() + " on node "
                + getTargetNode().getValue());

        SystemMetadata sysmeta = null;
        ReplicationStatus status = null;

        // variables for hzProcess component coordination only
        ILock lock = null;
        String lockString = null;
        boolean isLocked = false;

        // a flag showing an already existing replica on the target MN
        boolean exists = false;

        // a flag for success on setting replication status
        boolean success = false;

        boolean updated = false;

        // a flag showing if the replica entry was deleted due to comm problems
        boolean deleted = false;

        // session is null - certificate is used
        Session session = null;

        // Get an CNode reference to communicate with
        try {
            this.cn = D1Client.getCN();

        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the CN "
                    + "during replication task id " + getTaskid() + ", identifier "
                    + getPid().getValue() + ", target node " + getTargetNode().getValue());
            // try again, then fail
            try {
                Thread.sleep(5000L);
                this.cn = D1Client.getCN();

            } catch (ServiceFailure e1) {
                log.warn("Second ServiceFailure while getting a reference to the CN "
                        + "during replication task id " + getTaskid() + ", identifier "
                        + getPid().getValue() + ", target node " + getTargetNode().getValue(), e1);
                this.cn = null;
                success = false;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the CN "
                        + "during replication task id " + getTaskid() + ", identifier "
                        + getPid().getValue() + ", target node " + getTargetNode().getValue(), ie);
                this.cn = null;
                success = false;

            }
        }

        // Get an target MNode reference to communicate with
        try {
            this.targetMN = D1Client.getMN(this.targetNode);

        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the MN "
                    + "during replication task id " + getTaskid() + ", identifier "
                    + getPid().getValue() + ", target node " + getTargetNode().getValue());

            try {
                // wait 5 seconds and try again, else fail
                Thread.sleep(5000L);
                this.targetMN = D1Client.getMN(this.targetNode);

            } catch (ServiceFailure e1) {
                log.error("There was a problem calling replicate() on "
                        + getTargetNode().getValue() + " for identifier " + this.pid.getValue()
                        + " during " + " task id " + getTaskid(), e1);
                this.targetMN = null;
                success = false;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the MN "
                        + "during replication task id " + getTaskid() + ", identifier "
                        + getPid().getValue() + ", target node " + getTargetNode().getValue(), ie);
                this.targetMN = null;
                success = false;

            }
        }

        // now try to call MN.replicate()
        try {

            if (this.cn != null && this.targetMN != null) {
                // get the most recent system metadata for the pid
                sysmeta = cn.getSystemMetadata(session, pid);

                // check to be sure the replica is not requested or completed
                List<Replica> replicaList = sysmeta.getReplicaList();
                boolean handled = false;

                for (Replica replica : replicaList) {
                    NodeReference listedNode = replica.getReplicaMemberNode();
                    ReplicationStatus currentStatus = replica.getReplicationStatus();

                    if (listedNode == this.targetNode) {
                        if (currentStatus == ReplicationStatus.REQUESTED
                                || currentStatus == ReplicationStatus.COMPLETED) {
                            handled = true;
                            break;

                        }
                    } else {
                        continue;

                    }
                }
                // call for the replication

                if (!handled) {

                    // check if the object exists on the target MN already
                    try {
                        DescribeResponse description = this.targetMN.describe(getPid());
                        if (description.getDataONE_Checksum().equals(sysmeta.getChecksum())) {
                            exists = true;

                        }

                    } catch (NotFound nfe) {
                        // set the status to REQUESTED to avoid race conditions
                        // across CN threads handling replication tasks
                        status = ReplicationStatus.REQUESTED;

                        updated = this.cn.setReplicationStatus(getPid(), this.targetNode, status,
                                null);
                        log.debug("Task id " + this.getTaskid()
                                + " called setReplicationStatus() for identifier "
                                + this.pid.getValue() + ". updated result: " + updated);

                        success = this.targetMN.replicate(session, sysmeta, this.originatingNode);
                        log.info("Task id " + this.getTaskid()
                                + " called replicate() at targetNode " + this.targetNode.getValue()
                                + ", identifier " + this.pid.getValue() + ". Success: " + success);
                    }

                } else {
                    log.info("for task id " + this.getTaskid() + " replica is already handled for "
                            + this.targetNode.getValue() + ", identifier " + this.pid.getValue());

                }

            } else {
                log.error("Can't get system metadata: CNode object is null for " + " task id "
                        + getTaskid() + ", identifier " + getPid().getValue() + ", target node "
                        + getTargetNode().getValue());
                success = false;
            }

        } catch (BaseException e) {
            log.error(
                    "Caught base exception attempting to call replicate for pid: " + pid.getValue()
                            + " with exception: " + e.getDescription() + " and message: "
                            + e.getMessage(), e);
            try {
                log.info("The call to MN.replicate() failed for " + pid.getValue() + " on "
                        + this.targetNode.getValue() + ". Trying again in 5 seconds.");
                this.retryCount++;
                Thread.sleep(5000L);
                // get the most recent system metadata for the pid
                if (this.cn != null && this.targetMN != null) {
                    try {
                        Checksum checksum = this.targetMN.getChecksum(getPid(), sysmeta
                                .getChecksum().getAlgorithm());
                        exists = checksum.equals(sysmeta.getChecksum());

                    } catch (NotFound nf) {
                        sysmeta = cn.getSystemMetadata(session, pid);
                        success = targetMN.replicate(session, sysmeta, this.originatingNode);
                        log.info("Task id " + this.getTaskid()
                                + " called replicate() at targetNode " + this.targetNode.getValue()
                                + ", identifier " + this.pid.getValue() + ". Success: " + success);
                    }

                } else {
                    log.error("Can't get system metadata: CNode object is null for " + " task id "
                            + getTaskid() + ", identifier " + getPid().getValue()
                            + ", target node " + getTargetNode().getValue());
                    success = false;
                }

            } catch (BaseException e1) {
                log.error(
                        "Caught base exception attempting to call replicate for pid: "
                                + pid.getValue() + " with exception: " + e.getDescription()
                                + " and message: " + e.getMessage(), e);
                // still couldn't call replicate() successfully. fail.
                log.error("There was a second problem calling replicate() on "
                        + getTargetNode().getValue() + " for identifier " + getPid().getValue()
                        + " during " + " task id " + getTaskid(), e1);
                success = false;

            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while calling replicate() "
                        + "during replication task id " + getTaskid() + ", identifier "
                        + getPid().getValue() + ", target node " + getTargetNode().getValue(), ie);
                success = false;

            }

        } catch (Exception e) {
            log.error("Unknown exception during replication task id " + getTaskid()
                    + ", identifier " + getPid().getValue() + ", target node "
                    + getTargetNode().getValue() + ". Error message: " + e.getMessage(), e);
            success = false;

        }

        // set the replication status
        if (success) {
            status = ReplicationStatus.REQUESTED;

        } else {
            status = ReplicationStatus.FAILED;

        }

        // for replicas that already exist, just update the system metadata
        if (exists) {
            status = ReplicationStatus.COMPLETED;

        }

        // if the status hasn't already been updated, update it
        if (!updated) {

            // depending on the status, update the status or delete the entry
            // in the system metadata for this identifier
            if (this.cn != null) {

                if (!status.equals(ReplicationStatus.FAILED)) {
                    // make every effort to update the status correctly
                    try {
                        updated = this.cn.setReplicationStatus(session, this.pid, this.targetNode,
                                status, null);

                    } catch (BaseException be) {

                        // the replica has already completed from a different
                        // task
                        if (be instanceof InvalidRequest) {
                            log.warn(
                                    "Couldn't set the replication status to " + status.toString()
                                            + ", it may have possibly "
                                            + "already been set to completed for identifier "
                                            + this.pid.getValue() + " and target node "
                                            + this.targetNode.getValue() + ". The error was: "
                                            + be.getMessage(), be);
                            return this.pid.getValue();

                        }

                        // there's trouble communicating with the local CN
                        log.error("There was a problem setting the replication status to "
                                + status.toString() + "  for identifier " + this.pid.getValue()
                                + " during " + " MNReplicationTask id " + this.taskid);
                        // try the router CN address
                        updated = setReplicationStatus(session, pid, this.targetNode, status, null);
                    }

                } else {

                    // this task has failed. make every effort to delete the
                    // replica entry so the node prioritization is not skewed
                    // (due to factors not associated with the node)
                    try {
                        // call the local cn
                        deleted = this.cn.deleteReplicationMetadata(session, pid, targetNode,
                                sysmeta.getSerialVersion().longValue());

                    } catch (BaseException be) {
                        // err. get the latest system metadata and call the cn
                        if (be instanceof VersionMismatch) {
                            try {
                                sysmeta = this.cn.getSystemMetadata(pid);
                                deleted = this.cn.deleteReplicationMetadata(session, pid,
                                        targetNode, sysmeta.getSerialVersion().longValue());

                            } catch (BaseException e) {
                                // we're really having difficulties. try the
                                // round robin CN address
                                deleted = deleteReplicationMetadata(session, pid, this.targetNode);
                            }
                        }
                    }
                    // if we got to this state, something is very wrong with the
                    // CN environment. move on
                    if (!deleted) {
                        log.error("FAILED deletion of replica entry for identifier "
                                + pid.getValue() + " and target node id " + targetNode.getValue());
                    }
                }

            } else {
                if (!status.equals(ReplicationStatus.FAILED)) {
                    log.error("Task " + this.getTaskid()
                            + " can't update replica status for identifier " + this.pid.getValue()
                            + " on node " + this.targetNode.getValue() + " to " + status.toString()
                            + ". CNode reference is null, trying the router address.");
                    // try setting the status against the router address
                    updated = setReplicationStatus(session, pid, targetNode, status, null);

                } else {
                    log.error("Task " + this.getTaskid()
                            + " can't delete the replica entry for identifier "
                            + this.pid.getValue() + " and node " + this.targetNode.getValue()
                            + ". CNode reference is null, trying the router address.");
                    // try deleting the entry against the router address
                    deleted = deleteReplicationMetadata(session, pid, this.targetNode);

                }
            }
        }
        log.trace("METRICS:\tREPLICATION:\tEND QUEUE:\tPID:\t" + pid.getValue() + "\tNODE:\t"
                + targetNode.getValue() + "\tSIZE:\t" + sysmeta.getSize().intValue());

        if (updated) {
            log.info("Task " + this.getTaskid() + " updated replica status for identifier "
                    + this.pid.getValue() + " on node " + this.targetNode.getValue() + " to "
                    + status.toString());
            log.trace("METRICS:\tREPLICATION:\t" + status.toString().toUpperCase() + ":\tPID:\t"
                    + pid.getValue() + "\tNODE:\t" + targetNode.getValue() + "\tSIZE:\t"
                    + sysmeta.getSize().intValue());

        } else {
            log.info("Task " + this.getTaskid() + " didn't update replica status for identifier "
                    + this.pid.getValue() + " on node " + this.targetNode.getValue() + " to "
                    + status.toString());
        }

        if (deleted) {
            log.info("Task " + this.getTaskid() + " deleted replica entry for identifier "
                    + this.pid.getValue() + " and node " + this.targetNode.getValue());

        }

        return this.pid.getValue();
    }

    /*
     * Set the replication status against the router CN address instead of the
     * local CN via D1Client. This may help with local CN communication trouble.
     */
    private boolean setReplicationStatus(Session session, Identifier pid, NodeReference targetNode,
            ReplicationStatus status, BaseException failure) {
        log.warn("setReplicationStatus() called against the router CN address. "
                + " Is the local CN communicationg properly?");
        CNode cn;
        boolean updated = false;
        cn = new CNode(this.cnRouterHostname);

        // try multiple times since at this point we may be dealing with a lame
        // CN in the cluster and the RR may still point us to it
        for (int i = 0; i < 5; i++) {

            try {
                updated = cn.setReplicationStatus(session, pid, targetNode, status, null);
                if (updated) {
                    break;
                }
            } catch (BaseException be) {
                // the replica has already completed from a different task
                if (be instanceof InvalidRequest) {
                    log.warn("Couldn't set the replication status to " + status.toString()
                            + ", it may have possibly "
                            + "already been set to completed for identifier " + this.pid.getValue()
                            + " and target node " + this.targetNode.getValue()
                            + ". The error was: " + be.getMessage(), be);
                    return false;

                }
                if (log.isDebugEnabled()) {
                    log.debug(be);

                }
                log.error(
                        "Error in calling setReplicationStatus() at " + this.cnRouterHostname
                                + " for identifier " + pid.getValue() + ", target node "
                                + targetNode.getValue() + " and status of " + status.toString()
                                + ": " + be.getMessage(), be);
                continue;
            }

        }
        return updated;
    }

    /*
     * Delete the replica entry for the target node using the CN router URL
     * rather than the local CN via D1Client. This may help with local CN
     * communication trouble.
     * 
     * @param session - the CN session instance
     * 
     * @param - pid - the identifier of the object system metadata being
     * modified
     * 
     * @param targetNode - the node id of the replica target being deleted
     * 
     * @param serialVersion - the serialVersion of the system metadata being
     * operated on
     */
    private boolean deleteReplicationMetadata(Session session, Identifier pid,
            NodeReference targetNode) {

        SystemMetadata sysmeta;
        CNode cn;
        boolean deleted = false;
        cn = new CNode(this.cnRouterHostname);

        // try multiple times since at this point we may be dealing with a lame
        // CN in the cluster and the RR may still point us to it
        for (int i = 0; i < 5; i++) {
            try {
                // refresh the system metadata in case it changed
                sysmeta = cn.getSystemMetadata(pid);
                deleted = cn.deleteReplicationMetadata(pid, targetNode, sysmeta.getSerialVersion()
                        .longValue());
                if (deleted) {
                    break;
                }

            } catch (BaseException be) {
                if (log.isDebugEnabled()) {
                    log.debug(be);
                }
                log.error("Error in calling deleteReplicationMetadata() at "
                        + this.cnRouterHostname + " for identifier " + pid.getValue()
                        + " and target node " + targetNode.getValue() + ": " + be.getMessage(), be);
                continue;

            } catch (RuntimeException re) {
                if (log.isDebugEnabled()) {
                    log.debug(re);
                }
                log.error("Error in getting sysyem metadata from the map for " + "identifier "
                        + pid.getValue() + ": " + re.getMessage(), re);
                continue;

            }
        }

        return deleted;
    }

    /**
     * Implements the Runnable interface, but the task is actually called via
     * the Callable interface. This is needed as a placeholder to handle
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
     * @return retryCount the number of retries for this replication task
     */
    public int getRetryCount() {
        return this.retryCount;
    }

}
