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
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.CertificateManager;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

/**
 * A single audit task to be queued and executed by the Replication Service. The
 * audit task is generated from the result of a query on objects with replicas
 * that haven't been verified in 2 or more months.
 * 
 * @author dexternc
 * 
 */
public class MNAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = 203749838189419405L;

    public static Log log = LogFactory.getLog(MNAuditTask.class);

    private String taskid;

    private List<ReplicaAuditDto> replicasToAudit = new ArrayList<ReplicaAuditDto>();

    private int retryCount;

    public MNAuditTask(String taskid, List<ReplicaAuditDto> replicaAudits) {
        this.taskid = taskid;
        this.replicasToAudit = replicaAudits;
    }

    /**
     * Implement the Callable interface, providing code that initiates auditing.
     * 
     * @return pid - the identifier of the replicated object upon success
     */
    public String call() throws IllegalStateException {

        MNode targetMN = null;
        Identifier last_pid = null;

        // Get an target MNode reference to communicate with
        try {
            // set up the certificate location
            String clientCertificateLocation = Settings.getConfiguration().getString(
                    "D1Client.certificate.directory")
                    + File.separator
                    + Settings.getConfiguration().getString("D1Client.certificate.filename");
            CertificateManager.getInstance().setCertificateLocation(clientCertificateLocation);
            log.debug("MNReplicationTask task id " + this.taskid + "is using an X509 certificate "
                    + "from " + clientCertificateLocation);
            log.debug("Getting the MNode reference for " + auditTargetNode.getValue());
            targetMN = D1Client.getMN(auditTargetNode);

        } catch (ServiceFailure e) {
            log.debug("Failed to get the target MNode reference for " + auditTargetNode.getValue()
                    + " while executing MNAuditTask id " + this.taskid);
        }

        // Get the D1 Hazelcast configuration parameters
        String hzSystemMetadata = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");

        String auditReplicasHandledSetName = Settings.getConfiguration().getString(
                "dataone.hazelcast.replication.audit.processing.identifiers");

        // get the system metadata for the pid
        HazelcastClient hzClient = HazelcastClientInstance.getHazelcastClient();

        IMap<String, SystemMetadata> sysMetaMap = hzClient.getMap(hzSystemMetadata);
        Set<Identifier> handledReplicaAuditTasks = hzClient.getSet(auditReplicasHandledSetName);

        // Initiate the MN checksum verification
        try {

            // the system metadata for the object to be checked.
            SystemMetadata sysmeta;

            // for each pid in the list of pids to be checked on this node
            for (Identifier pid : auditIDs) {

                last_pid = pid;

                // lock the pid we are going to check
                sysMetaMap.lock(pid.getValue());

                // get the system metadata to modify ReplicationStatus and
                // Replica for
                // this identifier
                sysmeta = sysMetaMap.get(pid);

                // list of replicas to check
                ArrayList<Replica> replicaList = (ArrayList<Replica>) sysmeta.getReplicaList();

                // the index of the Replica in the List<Replica>
                int replicaIndex = -1;

                for (Replica replica : replicaList) {
                    if (replica.getReplicaMemberNode().getValue()
                            .equals(auditTargetNode.getValue())) {
                        replicaIndex = replicaList.indexOf(replica);
                        break;
                    }
                }

                if (replicaIndex == -1) {
                    throw new InvalidRequest("1080", "Node is not reported to have " + "object: "
                            + pid);
                }

                // call for the checksum audit
                log.debug("Calling MNRead.getChecksum() at auditTargetNode id "
                        + targetMN.getNodeId());

                // session is null - certificate is used
                Session session = null;

                // get the target checksum
                Checksum targetChecksum = targetMN.getChecksum(session, pid, "MD5");

                if (targetChecksum != sysmeta.getChecksum()) {
                    replicaList.get(replicaIndex).setReplicationStatus(
                            ReplicationStatus.INVALIDATED);
                    replicaList.get(replicaIndex).setReplicaVerified(
                            Calendar.getInstance().getTime());

                } else {
                    replicaList.get(replicaIndex).setReplicaVerified(
                            Calendar.getInstance().getTime());
                }

                sysmeta.setReplicaList(replicaList);
                sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
                sysMetaMap.unlock(pid.getValue());
                handledReplicaAuditTasks.remove(pid);
            }

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

        } catch (InvalidToken e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } catch (NotFound e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        } finally {
            sysMetaMap.unlock(last_pid.getValue());
        }

        return auditTargetNode.getValue();
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
}
