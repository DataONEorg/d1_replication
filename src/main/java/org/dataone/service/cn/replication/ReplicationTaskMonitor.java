package org.dataone.service.cn.replication;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.cn.ComponentActivationUtility;
import org.dataone.cn.data.repository.ReplicationTask;
import org.dataone.cn.data.repository.ReplicationTaskRepository;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.log.MetricEvent;
import org.dataone.cn.log.MetricLogClientFactory;
import org.dataone.cn.log.MetricLogEntry;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

public class ReplicationTaskMonitor implements Runnable {

    private static Logger log = Logger.getLogger(ReplicationTaskMonitor.class);

    //XXX why is this static when we are getting them from a Factory?
    //XXX (or why is the Factory exhibiting monostate behavior instead of building new things?)  
    private static ReplicationTaskRepository taskRepository = ReplicationFactory
            .getReplicationTaskRepository();

    
    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadataMap = HazelcastClientFactory.getSystemMetadataMap();
    
    
    private Map<String,Counter> statusCount = new HashMap<>();
    private Map<NodeReference,Counter> authMNCount = new HashMap<>();
    
    /**
     * queries the replication task repository for a page of 'NEW' tasks, and
     * sends them to the ReplicationManager for execution. 
     */
    @Override
    public void run() {
        log.debug("Replication task monitoring executing.");

        Iterable<ReplicationTask> allTasks = taskRepository.findAll();
        if (allTasks != null) {
            Iterator<ReplicationTask> it = allTasks.iterator();
            while (it.hasNext()) {
                ReplicationTask task = it.next();
                processCounters(task);
            }
            // report tasks by Status
            for (Entry<String,Counter> n : statusCount.entrySet()) {
                MetricLogEntry metricLogEntry = new MetricLogEntry(MetricEvent.REPLICATION_TASKS);
                metricLogEntry.setMessage(
                        String.format("Total Replication Tasks %s: %d", 
                                n.getKey(), 
                                n.getValue().getCount())
                                );
                MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
            }
            
            // report tasks by AuthMN
            for (Entry<NodeReference,Counter> n : authMNCount.entrySet()) {
                MetricLogEntry metricLogEntry = new MetricLogEntry(MetricEvent.REPLICATION_TASKS, n.getKey());
                metricLogEntry.setMessage(
                        String.format("Replication Tasks: %d", 
                                n.getValue().getCount())
                                );
                MetricLogClientFactory.getMetricLogClient().logMetricEvent(metricLogEntry);
            }
            
        }
    }

    private void processCounters(ReplicationTask task) {

        // create counts by status
        String status = task.getStatus();
        try {
            statusCount.get(status).increment();
        } 
        catch (NullPointerException e) {
            statusCount.put(status, new Counter());
            statusCount.get(status).increment();
        }
        
        NodeReference authMN = getAuthoritativeMN(task.getPid());
        try {
            authMNCount.get(authMN).increment();
        }
        catch (NullPointerException e) {
            authMNCount.put(authMN, new Counter());
            authMNCount.get(authMN).increment();
        }
    }

    private NodeReference getAuthoritativeMN(String pid) {
        // lookup authMN in systemMetadata
        try {
            return this.systemMetadataMap.get(TypeFactory.buildIdentifier(pid)).getAuthoritativeMemberNode();
        }
        catch (NullPointerException e) {
            // if we get nulls along the way...
            return null;
        }
    }
    
    
    
    private class Counter {
        private int count = 0;
        
        void increment() {
            ++count;
        }
        
        int getCount() {
            return count;
        }
    }
}

