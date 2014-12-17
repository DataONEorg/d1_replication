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
package org.dataone.service.cn.replication;

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v2.SystemMetadata;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;

/**
 * An event listener used to manage change events on the hzSystemMetadata map.
 * The listener queues Identifiers into the hzReplicationEvents queue when the
 * system metadata map is added to or updated. The events queue is monitored for
 * additions, and the ReplicationManager is called to evaluate replication
 * policies for the given identifier and potentially create replication tasks.
 * 
 * @author cjones
 * 
 */
public class ReplicationEventListener implements EntryListener<Identifier, SystemMetadata> {

    /* Get a Log instance */
    private static Logger log = Logger.getLogger(ReplicationEventListener.class);

    /* The instance of the Hazelcast storage cluster client */
    private HazelcastClient hzClient;

    /* The name of the system metadata map */
    private static final String systemMetadataMap = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadata;

    private ReplicationTaskRepository replicationTaskRepository;

    /**
     * Constructor: create a replication event listener that listens for entry
     * events on the hzSystemMetadata map a queues the identifier key for
     * replication task creation. Intended to be used by the ReplicationManager
     * to manage hzSystemMetadata map events
     */
    public ReplicationEventListener() {
        // connect to both the process and storage cluster
        this.hzClient = HazelcastClientFactory.getStorageClient();
        // get references to the system metadata map and events queue
        this.systemMetadata = this.hzClient.getMap(systemMetadataMap);
        // listen for changes on system metadata
        this.systemMetadata.addEntryListener(this, true);
        log.info("Added a listener to the " + this.systemMetadata.getName() + " map.");
        this.replicationTaskRepository = ReplicationFactory.getReplicationTaskRepository();
        // start replication manager
        ReplicationFactory.getReplicationManager();
    }

    /**
     * Initialize the bean object
     */
    public void init() {
        log.info("initialization");
    }

    /**
     * Implement the EntryListener interface, responding to entries being added
     * to the hzSystemMetadata map.
     * 
     * @param event
     *            - the entry event being added to the map
     */
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {

        if (ComponentActivationUtility.replicationIsActive()) {
            log.info("Received entry added event on the hzSystemMetadata map for pid: "
                    + event.getKey().getValue());

            if (isAuthoritativeReplicaValid(event.getValue())) {
                createReplicationTask(event.getKey());
            } else {
                log.info("Authoritative replica is not valid, not queueing to replication for pid: "
                        + event.getKey().getValue());
            }
        }
    }

    /**
     * Implement the EntryListener interface, responding to entries being
     * updated in the hzSystemMetadata map.
     * 
     * @param event
     *            - the entry event being updated in the map
     */
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {

        if (ComponentActivationUtility.replicationIsActive()) {
            log.info("Received entry updated event on the hzSystemMetadata map for pid: "
                    + event.getKey().getValue());

            if (isAuthoritativeReplicaValid(event.getValue())) {
                createReplicationTask(event.getKey());
            } else {
                log.info("Authoritative replica is not valid, not queueing to replication for pid: "
                        + event.getKey().getValue());
            }
        }
    }

    private void createReplicationTask(Identifier identifier) {
        if (identifier == null || identifier.getValue() == null) {
            log.error("Replication Event Listener received event with null identifier");
            return;
        }
        List<ReplicationTask> existingTaskList = replicationTaskRepository.findByPid(identifier
                .getValue());
        if (existingTaskList.isEmpty()) {
            replicationTaskRepository.save(new ReplicationTask(identifier));
        } else if (existingTaskList.size() > 1) {
            log.error("Found more than one replication task object for pid:"
                    + identifier.getValue() + ".  Deleting and creating new task.");
            replicationTaskRepository.delete(existingTaskList);
            replicationTaskRepository.save(new ReplicationTask(identifier));
        }
    }

    private boolean isAuthoritativeReplicaValid(SystemMetadata sysMeta) {
        if (sysMeta == null) {
            return false;
        }
        ReplicationStatus status = getAuthoritativeMNReplicaStatus(sysMeta);
        return ReplicationStatus.COMPLETED.equals(status);
    }

    private ReplicationStatus getAuthoritativeMNReplicaStatus(SystemMetadata sysMeta) {
        NodeReference authNode = sysMeta.getAuthoritativeMemberNode();
        for (Replica replica : sysMeta.getReplicaList()) {
            if (authNode.equals(replica.getReplicaMemberNode())) {
                return replica.getReplicationStatus();
            }
        }
        return null;
    }

    /**
     * Implement the EntryListener interface, responding to entries being
     * deleted from the hzSystemMetadata map.
     * 
     * @param event
     *            - the entry event being deleted from the map
     */
    public void entryRemoved(EntryEvent<Identifier, SystemMetadata> event) {
        // we don't remove replicas (do we?)

    }

    /**
     * Implement the EntryListener interface, responding to entries being
     * evicted from the hzSystemMetadata map.
     * 
     * @param event
     *            - the entry event being evicted from the map
     */
    public void entryEvicted(EntryEvent<Identifier, SystemMetadata> event) {
        // nothing to do, entry remains in backing store

    }
}
