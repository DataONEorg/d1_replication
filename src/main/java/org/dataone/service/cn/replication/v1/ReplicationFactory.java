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

import org.dataone.cn.data.repository.ReplicationAttemptHistoryRepository;
import org.dataone.cn.data.repository.ReplicationPostgresRepositoryFactory;
import org.dataone.cn.data.repository.ReplicationTaskRepository;

/**
 * Factory class to provide consumers handle on replication objects
 * 
 * @author sroseboo
 * 
 */
public class ReplicationFactory {

    private static ReplicationManager replicationManager;
    private static ReplicationService replicationService;
    private static ReplicationTaskQueue replicationTaskQueue;
    private static ReplicationRepositoryFactory repositoryFactory;
    private static ReplicationAttemptHistoryRepository tryHistoryRepository;
    private static ReplicationTaskRepository replicationTaskRepository;

    private ReplicationFactory() {
    }

    public static ReplicationManager getReplicationManager() {
        if (replicationManager == null) {
            replicationManager = new ReplicationManager();
        }
        return replicationManager;
    }

    public static ReplicationService getReplicationService() {
        if (replicationService == null) {
            replicationService = new ReplicationService();
        }
        return replicationService;
    }

    public static ReplicationTaskQueue getReplicationTaskQueue() {
        if (replicationTaskQueue == null) {
            replicationTaskQueue = new ReplicationTaskQueue();
        }
        return replicationTaskQueue;
    }

    public static ReplicationAttemptHistoryRepository getReplicationTryHistoryRepository() {
        if (repositoryFactory == null) {
            repositoryFactory = new ReplicationPostgresRepositoryFactory();
            repositoryFactory.initContext();
        }
        if (tryHistoryRepository == null) {
            tryHistoryRepository = repositoryFactory.getReplicationTryHistoryRepository();
        }
        return tryHistoryRepository;
    }

    public static ReplicationTaskRepository getReplicationTaskRepository() {
        if (repositoryFactory == null) {
            repositoryFactory = new ReplicationPostgresRepositoryFactory();
            repositoryFactory.initContext();
        }
        if (replicationTaskRepository == null) {
            replicationTaskRepository = repositoryFactory.getReplicationTaskRepository();
        }
        return replicationTaskRepository;
    }
}
