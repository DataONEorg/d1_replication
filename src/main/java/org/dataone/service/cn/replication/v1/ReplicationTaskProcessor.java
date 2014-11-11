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

import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.client.D1TypeBuilder;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;

public class ReplicationTaskProcessor implements Runnable {

    private static Logger log = Logger.getLogger(ReplicationTaskProcessor.class);

    private static ReplicationTaskRepository taskRepository = ReplicationFactory
            .getReplicationTaskRepository();
    private static ReplicationManager replicationManager = ReplicationFactory
            .getReplicationManager();

    @Override
    public void run() {
        if (ComponentActivationUtility.replicationIsActive()) {
            long now = System.currentTimeMillis();
            List<ReplicationTask> taskList = taskRepository.findByStatusAndNextExecutionLessThan(
                    ReplicationTask.STATUS_NEW, now);
            for (ReplicationTask task : taskList) {
                task.markInProcess();
                taskRepository.save(task);
                replicationManager
                        .createAndQueueTasks(D1TypeBuilder.buildIdentifier(task.getPid()));
            }
        }
    }
}
