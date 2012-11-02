/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
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
 */
package org.dataone.service.cn.replication.v1;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.D1TypeBuilder;
import org.dataone.service.types.v1.NodeReference;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MultiMap;

/**
 * Abstract member node replication task work queue. Provides interface for
 * registering an entry listener, adding tasks, getting tasks. Encapsulates
 * entry listening strategies.
 * 
 * @author sroseboo
 * 
 */
public class ReplicationTaskQueue implements EntryListener<String, MNReplicationTask> {

    private static Log log = LogFactory.getLog(ReplicationTaskQueue.class);
    private static MultiMap<String, MNReplicationTask> replicationTaskMap;

    /*
     * has this instance of replication task queue been registered as a entry
     * listener
     */
    private boolean listening = false;

    static {
        replicationTaskMap = Hazelcast.getDefaultInstance()
                .getMultiMap("hzReplicationTaskMultiMap");
    }

    public ReplicationTaskQueue() {
    }

    public void registerAsEntryListener() {
        if (!this.listening) {
            replicationTaskMap.addEntryListener(this, true);
            this.listening = true;
            log.info("Added a listener to the " + replicationTaskMap.getName() + " queue.");
        }
    }

    public void addTask(MNReplicationTask task) {
        replicationTaskMap.put(task.getTargetNode().getValue(), task);
    }

    public Collection<MNReplicationTask> getAllTasks() {
        return replicationTaskMap.values();
    }

    public Collection<NodeReference> getMemberNodesInQueue() {
        Set<NodeReference> memberNodes = new HashSet<NodeReference>();
        for (String nodeId : replicationTaskMap.keySet()) {
            memberNodes.add(D1TypeBuilder.buildNodeReference(nodeId));
        }
        return memberNodes;
    }

    public int getCountOfTasksForNode(String nodeId) {
        return replicationTaskMap.valueCount(nodeId);
    }

    @Override
    public void entryAdded(EntryEvent<String, MNReplicationTask> event) {
        if (ReplicationUtil.replicationIsActive()) {
            processAllTasksForMN(event.getKey());
            log.debug("ReplicationTaskQueue. Handling item added event.");
        }
    }

    public void processAllTasksForMN(String memberNodeIdentifierValue) {
        String mnId = memberNodeIdentifierValue;
        if (mnId != null) {
            log.debug("ReplicationTaskQueue. Processing all tasks for node: " + mnId + ".");
            if (replicationTaskMap.valueCount(mnId) > 0) {
                Collection<MNReplicationTask> tasks = removeTasksForMemberNode(mnId);
                if (tasks != null && tasks.isEmpty() == false) {
                    for (MNReplicationTask task : tasks) {
                        if (task != null) {
                            try {
                                this.executeTask(task);
                            } catch (Exception e) {
                                log.debug("Caught exception executing task id " + task.getTaskid()
                                        + ": " + e.getMessage());
                                if (log.isDebugEnabled()) {
                                    log.debug(e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private Collection<MNReplicationTask> removeTasksForMemberNode(String memberNodeId) {
        Collection<MNReplicationTask> tasks = null;
        boolean locked = false;
        try {
            locked = replicationTaskMap.tryLock(memberNodeId, 3L, TimeUnit.SECONDS);
            if (locked) {
                tasks = replicationTaskMap.remove(memberNodeId);
            }
        } catch (Exception e) {
            log.debug("Caught exception trying to use mn replication task map", e);
        } finally {
            if (locked) {
                replicationTaskMap.unlock(memberNodeId);
            }
        }
        return tasks;
    }

    /**
     * Encapsulates implementation details on how a replication task is
     * executed.
     * 
     * @param task
     */
    private void executeTask(MNReplicationTask task) {
        log.debug("Executing task id " + task.getTaskid() + "for identifier "
                + task.getPid().getValue() + " and target node " + task.getTargetNode().getValue());
        task.call();
    }

    @Override
    public void entryRemoved(EntryEvent<String, MNReplicationTask> event) {
        // Not implemented.
    }

    @Override
    public void entryUpdated(EntryEvent<String, MNReplicationTask> event) {
        // Not implemented.
    }

    @Override
    public void entryEvicted(EntryEvent<String, MNReplicationTask> event) {
        // Not implemented.
    }
}
