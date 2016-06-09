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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.cn.log.MetricEvent;
import org.dataone.cn.log.MetricLogClientFactory;
import org.dataone.cn.log.MetricLogEntry;
import org.dataone.service.types.v2.TypeFactory;

/**
 * 
 * Monitors the replica (table) to summarize the number of replicas
 * by target node and replication status.
 * 
 * @author rnahf
 *
 */
public class ReplicationStatusMonitor implements Runnable {

    private static Logger log = Logger.getLogger(ReplicationStatusMonitor.class);

    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();

    public ReplicationStatusMonitor() {
    }

    @Override
    public void run() {
        try {
            Map<String, Integer> summary = replicationDao.getCountsByNodeStatus();
            for (Entry<String, Integer> n : summary.entrySet()) {
                String[] nodeStatusPair = StringUtils.split(n.getKey(),"-");
                String nodeId = nodeStatusPair[0];
                String status = nodeStatusPair[1];
                
                MetricLogEntry metricLogEntry = new MetricLogEntry(
                        MetricEvent.REPLICA_STATUS, TypeFactory.buildNodeReference(nodeId));
                metricLogEntry.setMessage(
                        String.format("Replica Status %s:  %d", status, n.getValue()));

                MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
                n.getValue();
            }
            
            
        } catch (DataAccessException e) {
            log.warn("DAOAccessException while trying to get Replica status statistics" +
                    " with message: " + e.getMessage(),e);
        }
        
    }
}
