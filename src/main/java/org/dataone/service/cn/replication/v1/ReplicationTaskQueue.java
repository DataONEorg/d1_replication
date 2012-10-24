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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    private MultiMap<String, MNReplicationTask> replicationTaskMap;
    private boolean listening = false;

    public ReplicationTaskQueue() {
        this.replicationTaskMap = Hazelcast.getDefaultInstance().getMultiMap(
                "hzReplicationTaskMultiMap");
    }

    public void registerAsEntryListener() {
        if (!this.listening) {
            this.replicationTaskMap.addEntryListener(this, true);
            this.listening = true;
            log.info("Added a listener to the " + this.replicationTaskMap.getName() + " queue.");
        }
    }

    public void addTask(MNReplicationTask task) {
        this.replicationTaskMap.put(task.getTargetNode().getValue(), task);
    }

    public Collection<MNReplicationTask> getAllTasks() {
        return this.replicationTaskMap.values();
    }

    @Override
    public void entryAdded(EntryEvent<String, MNReplicationTask> event) {
        processAllTasksForMN(event.getKey());
    }

    private void processAllTasksForMN(String memberNodeIdentifierValue) {
        String mnId = memberNodeIdentifierValue;
        if (mnId != null) {
            log.debug("ReplicationManager entryAdded.  Processing all tasks for node: " + mnId
                    + ".");
            if (this.replicationTaskMap.valueCount(mnId) > 0) {
                boolean locked = this.replicationTaskMap.tryLock(mnId, 3L, TimeUnit.SECONDS);
                if (locked) {
                    Collection<MNReplicationTask> tasks = this.replicationTaskMap.remove(mnId);
                    this.replicationTaskMap.unlock(mnId);
                    for (MNReplicationTask task : tasks) {
                        if (task != null) {
                            log.debug("Executing task id " + task.getTaskid() + "for identifier "
                                    + task.getPid().getValue() + " and target node "
                                    + task.getTargetNode().getValue());
                            try {
                                String result = task.call();
                                log.debug("Result of executing task id" + task.getTaskid()
                                        + " is identifier string: " + result);
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

    private void processAnyReplicationTask() {
        boolean processedTask = false;
        for (String nodeId : this.replicationTaskMap.keySet()) {
            processedTask = processMNReplicationTask(nodeId);
            if (processedTask) {
                break;
            }
        }
    }

    private boolean processMNReplicationTask(String mnId) {
        boolean processedTask = false;
        if (this.replicationTaskMap.valueCount(mnId) > 0) {
            boolean locked = this.replicationTaskMap.tryLock(mnId, 3L, TimeUnit.SECONDS);
            if (locked) {
                Collection<MNReplicationTask> tasks = this.replicationTaskMap.get(mnId);
                MNReplicationTask task = null;
                if (tasks != null && tasks.iterator().hasNext()) {
                    task = tasks.iterator().next();
                }
                if (task != null) {
                    this.replicationTaskMap.remove(mnId, task);
                    this.replicationTaskMap.unlock(mnId);
                    log.debug("Executing task id " + task.getTaskid() + "for identifier "
                            + task.getPid().getValue() + " and target node "
                            + task.getTargetNode().getValue());
                    try {
                        String result = task.call();
                        processedTask = true;
                        log.debug("Result of executing task id" + task.getTaskid()
                                + " is identifier string: " + result);
                    } catch (Exception e) {
                        log.debug("Caught exception executing task id " + task.getTaskid() + ": "
                                + e.getMessage());
                        if (log.isDebugEnabled()) {
                            log.debug(e);
                        }
                    }
                } else {
                    this.replicationTaskMap.unlock(mnId);
                }
            } else {
                log.debug("ReplicationManager processMNReplicationTask - unable to aquire map lock for MN: "
                        + mnId);
            }
        }
        return processedTask;
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
