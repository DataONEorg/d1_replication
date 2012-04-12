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

package org.dataone.service.cn.replication.audit;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.v1.ReplicationManager;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * A single audit task to be queued and executed by the Replication Service. The
 * audit task is generated from the result of a query on objects with replicas
 * that haven't been verified in 2 or more months.
 * 
 * @author sroseboo
 * 
 */
public class ReplicationAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = 203749838189419405L;

    public static Log log = LogFactory.getLog(ReplicationAuditTask.class);

    private String taskid;

    private List<Identifier> pidsToAudit = new ArrayList<Identifier>();

    private int retryCount;

    private CNode cn;

    private Map<NodeReference, MNode> mnMap = new HashMap<NodeReference, MNode>();

    public ReplicationAuditTask(String taskid, List<Identifier> pids) {
        this.taskid = taskid;
        this.pidsToAudit = pids;

    }

    public String call() throws IllegalStateException {

        configureCertificate();
        createCNode();
        if (cn == null) {
            log.error("Unable to process replication audit tasks due to failure to aquire handle to CNode object.");
            return "FAILURE - replication audit task, unable to create CNode client.";
        }
        for (Identifier pid : pidsToAudit) {
            boolean queueToReplication = false;
            SystemMetadata sysMeta = getSystemMetadataFromCN(pid);
            if (sysMeta == null) {
                log.error("Cannot get system metadata from CN: " + cn.getNodeId() + " for pid: "
                        + pid + "Could not audit replication.");
                // TODO: no sysmeta - queue to replication?
                continue;
            }
            int validReplicaCount = 0;
            String cnChecksumValue = sysMeta.getChecksum().getValue();

            for (Replica replica : sysMeta.getReplicaList()) {
                MNode mn = getMNode(replica.getReplicaMemberNode());
                if (mn == null) {
                    log.error("Cannot get MN: " + replica.getReplicaMemberNode().getValue()
                            + " unable to verify replica information.");
                    // TODO: how to handle not finding the MN? is the replica
                    // INVALID? MN down for maintenance, temporary outage?
                    continue;
                }
                Checksum mnChecksum = getChecksumFromMN(pid, sysMeta, mn);
                if (mnChecksum == null) {
                    log.error("Cannot get checksum for pid: " + pid + " from MN: "
                            + replica.getReplicaMemberNode().getValue());
                    handleInvalidReplica(pid, sysMeta, replica);
                    queueToReplication = true;
                }
                if (mnChecksum.getValue().equals(cnChecksumValue)) {
                    validReplicaCount++;
                    replica.setReplicaVerified(new Date(System.currentTimeMillis()));
                    boolean success = updateReplica(pid, sysMeta, replica);
                    if (!success) {
                        log.error("Cannot update replica verified date  for pid: " + pid
                                + " on CN: " + cn.getNodeId());
                        queueToReplication = true;
                    }
                } else {
                    log.error("Checksum mismatch for pid: " + pid + " against MN: "
                            + replica.getReplicaMemberNode() + " CN checksum: " + cnChecksumValue
                            + " MN checksum: " + mnChecksum.getValue());
                    handleInvalidReplica(pid, sysMeta, replica);
                    queueToReplication = true;
                }
                if (queueToReplication) {
                    break;
                }
            }
            if (shouldSendToReplication(queueToReplication, sysMeta, validReplicaCount)) {
                boolean success = sendToReplication(pid);
                if (!success) {
                    log.error("Cannot queue pid: " + pid + " to replication queue on CN: ."
                            + cn.getNodeId());
                    continue;
                }
            }
        }
        return "Replica audit task: " + taskid + " on CNode: " + cn.getNodeId() + "for pids: "
                + pidsToAudit + " completed.";
    }

    private boolean shouldSendToReplication(boolean queueToReplication, SystemMetadata sysMeta,
            int validReplicaCount) {
        return queueToReplication
                || validReplicaCount != sysMeta.getReplicationPolicy().getNumberReplicas()
                        .intValue();
    }

    private void handleInvalidReplica(Identifier pid, SystemMetadata sysMeta, Replica replica) {
        replica.setReplicationStatus(ReplicationStatus.INVALIDATED);
        boolean success = updateReplica(pid, sysMeta, replica);
        if (!success) {
            log.error("Cannot update replica status to INVALID for pid: " + pid + " on MN: "
                    + replica.getReplicaMemberNode().getValue());
        }
    }

    private boolean sendToReplication(Identifier pid) {
        try {
            ReplicationManager.INSTANCE.createAndQueueTasks(pid);
            return true;
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotImplemented e) {
            e.printStackTrace();
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (InvalidRequest e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean updateReplica(Identifier pid, SystemMetadata sysMeta, Replica replica) {
        try {
            cn.updateReplicationMetadata(null, pid, replica, sysMeta.getSerialVersion().longValue());
            return true;
        } catch (NotImplemented e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        } catch (InvalidRequest e) {
            e.printStackTrace();
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (VersionMismatch e) {
            e.printStackTrace();
        }
        return false;
    }

    private Checksum getChecksumFromMN(Identifier pid, SystemMetadata sysMeta, MNode mn) {
        try {
            return mn.getChecksum(null, pid, sysMeta.getChecksum().getAlgorithm());
        } catch (InvalidRequest e) {
            e.printStackTrace();
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (NotImplemented e) {
            e.printStackTrace();
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        }
        return null;
    }

    private SystemMetadata getSystemMetadataFromCN(Identifier pid) {
        try {
            return cn.getSystemMetadata(null, pid);
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        } catch (NotImplemented e) {
            e.printStackTrace();
        }
        return null;
    }

    private MNode getMNode(NodeReference nodeRef) {
        if (!mnMap.containsKey(nodeRef)) {
            MNode mn = null;
            try {
                mn = D1Client.getMN(nodeRef);
            } catch (ServiceFailure e) {
                log.warn("Caught a ServiceFailure while getting a reference to the MN "
                        + nodeRef.getValue() + " during replication task id " + getTaskid());
                try {
                    // wait and try again, else fail
                    Thread.sleep(3000);
                    mn = D1Client.getMN(nodeRef);
                } catch (ServiceFailure e1) {
                    log.error("Caught a ServiceFailure while getting a reference to the MN "
                            + nodeRef.getValue() + " during replication task id " + getTaskid());
                    e1.printStackTrace();
                } catch (InterruptedException ie) {
                    log.error("Caught a ServiceFailure while getting a reference to the MN "
                            + nodeRef.getValue() + " during replication task id " + getTaskid());
                    ie.printStackTrace();
                }
            }
            mnMap.put(nodeRef, mn);
        }
        return mnMap.get(nodeRef);
    }

    private void createCNode() {
        try {
            cn = D1Client.getCN();
        } catch (ServiceFailure e) {
            log.warn("Caught a ServiceFailure while getting a reference to the CN "
                    + "during replica audit task id " + getTaskid());
            // try again, then fail
            try {
                Thread.sleep(3000);
                cn = D1Client.getCN();
            } catch (ServiceFailure e1) {
                log.warn("Second ServiceFailure while getting a reference to the CN "
                        + "during replica audit task id " + getTaskid());
                e1.printStackTrace();
            } catch (InterruptedException ie) {
                log.error("Caught InterruptedException while getting a reference to the CN "
                        + "during replica audit task id " + getTaskid());
                ie.printStackTrace();
            }
        }
    }

    private void configureCertificate() {
        // set up the certificate location
        String clientCertificateLocation = Settings.getConfiguration().getString(
                "D1Client.certificate.directory")
                + File.separator
                + Settings.getConfiguration().getString("D1Client.certificate.filename");
        CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
        log.info("MNReplicationTask task id " + this.taskid + " is using an X509 certificate "
                + "from " + clientCertificateLocation);
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getTaskid() {
        return taskid;
    }

    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }

    public List<Identifier> getPidsToAudit() {
        return pidsToAudit;
    }
}
