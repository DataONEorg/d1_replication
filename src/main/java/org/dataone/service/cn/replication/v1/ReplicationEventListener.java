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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ItemListener;
import com.hazelcast.client.HazelcastClient;
import org.dataone.cn.hazelcast.HazelcastClientInstance;

/**
 * An event listener used to manage change events on the hzSystemMetadata map.
 * The listener queues Identifiers into the hzReplicationEvents queue when the 
 * system metadata map is added to or updated.  The events queue is monitored
 * for additions, and the ReplicationManager is called to evaluate replication
 * policies for the given identifier and potentially create replication tasks.
 * 
 * @author cjones
 *
 */
public class ReplicationEventListener 
    implements EntryListener<Identifier, SystemMetadata>, ItemListener<Identifier> {

    /* A prefix appended to an identifier for coordinated locks across ReplicationManagers */
    private static final String EVENT_PREFIX = "replication-event-";

    /* Get a Log instance */
    public static Log log = LogFactory.getLog(ReplicationEventListener.class);

    /* The instance of the Hazelcast storage cluster client */
    private HazelcastClient hzClient;

    /* the instance of the Hazelcast processing cluster member */
    private HazelcastInstance hzMember;
            
    /* The name of the replication events queue */
    private String eventsQueue;

    /* The name of the system metadata map */
    private String systemMetadataMap;
    
    /* The Hazelcast distributed system metadata map */
    private IMap<Identifier, SystemMetadata> systemMetadata;
    
    /* The ReplicationManager instance */
    ReplicationManager replicationManager;
    
    /* The Hazelcast distributed replication events queue*/
    private IQueue<Identifier> replicationEvents;

        
    /**
     * Constructor: create a replication event listener that listens for entry
     * events on the hzSystemMetadata map a queues the identifier key for 
     * replication task creation. Intended to be used by the ReplicationManager
     * to manage hzSystemMetadata map events 
     */
    public ReplicationEventListener() {
        // connect to both the process and storage cluster
        this.hzClient = HazelcastClientInstance.getHazelcastClient();
        this.hzMember = Hazelcast.getDefaultInstance();
        this.eventsQueue =
            Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedEvents");
        // get references to the system metadata map and events queue
        this.systemMetadataMap =
            Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
        this.systemMetadata = this.hzClient.getMap(systemMetadataMap);
        this.replicationEvents = this.hzMember.getQueue(eventsQueue);

        // listen for changes on both structures
        this.systemMetadata.addEntryListener(this, true);
        log.info("Added a listener to the " + this.systemMetadata.getName() + " map.");
        this.replicationEvents.addItemListener(this, true);
        log.info("Added a listener to the " + this.replicationEvents.getName() + " queue.");
        
      
    }
    
    /**
     * Initialize the bean object
     */
    public void init() {
        log.info("initialization");
        
    }
    
    /**
     * Listen for item added events on the hzReplicationEvents queue.  Call 
     * the replicationManager to evaluate the replication policy for the identifier
     */
    public void itemAdded(Identifier identifier) {
        log.info("Item added event received on the hzReplicationEvents queue for " 
            + identifier.getValue());
        try {
            // poll the queue to pop the most recent event off of the queue
            Identifier pid = this.replicationEvents.poll(3L, TimeUnit.SECONDS);
            if ( pid != null ) {                
                // evaluate the object's replication policy for potential task creation
                this.replicationManager.createAndQueueTasks(identifier);
                
            }
            
        } catch (BaseException e) {
            log.error("There was a problem handling task creation for " + 
                identifier.getValue() + ". The error message was " +
                e.getMessage());
            e.printStackTrace();
            
        } catch (InterruptedException e) {
            log.debug("Plooing of the hzReplicationEvents queue was interrupted.");

        }       
    }

    /**
     * Listen for item removed events on the hzReplicationEvents queue. 
     */
    public void itemRemoved(Identifier identifier) {
        // nothing to do
        
    }

    /**
     * Implement the EntryListener interface, responding to entries being added to
     * the hzSystemMetadata map.
     * 
     * @param event - the entry event being added to the map
     */
    public void entryAdded(EntryEvent<Identifier, SystemMetadata> event) {
        log.info("Received entry added event on the hzSystemMetadata map. Queueing " + 
            event.getKey().getValue());
                
        // a lock to coordinate event handling between the 3 CN ReplicationManager instances
        String lockString = EVENT_PREFIX + event.getKey().getValue();
        Lock lock = null;
        boolean isLocked = false;
        
        try {
          
            // lock the event string and queue the event. 
            lock = this.hzMember.getLock(lockString);
            isLocked = lock.tryLock(500L, TimeUnit.MILLISECONDS);
            if (isLocked) {
               log.info("Locked " + event.getKey().getValue());
               
               queueEvent(event.getKey());
               
            } else {
                log.info("Didn't get lock for identifier " + event.getKey().getValue());
                
            }
                  
        } catch (NullPointerException e) {
            log.debug("The event identifier was null");
            
        } catch (RuntimeException e) {
            log.debug("Couldn't get a lock for " + lockString);
            e.printStackTrace();
        
        } catch (InterruptedException e) {
            log.debug("Lock retreival was interrupted for " + lockString);
            e.printStackTrace();
          
        } finally {
            if (isLocked) {
              lock.unlock();
              log.debug("Unlocked " + lockString);
              
            }           
        }          
    }


    /**
     * Implement the EntryListener interface, responding to entries being deleted from
     * the hzSystemMetadata map.
     * 
     * @param event - the entry event being deleted from the map
     */
    public void entryRemoved(EntryEvent<Identifier, SystemMetadata> event) {
      // we don't remove replicas (do we?) 
      
    }
    

    /**
     * Implement the EntryListener interface, responding to entries being updated in
     * the hzSystemMetadata map.
     * 
     * @param event - the entry event being updated in the map
     */
    public void entryUpdated(EntryEvent<Identifier, SystemMetadata> event) {
      
        log.info("Received entry updated event on the hzSystemMetadata map. Queueing " + 
            event.getKey().getValue());
                    
        // a lock to coordinate event handling between the 3 CN ReplicationManager instances
        String lockString = "replication-event-" + event.getKey().getValue();
        Lock lock = null;
        boolean isLocked = false;
        
        try {
          
            // lock the pid and queue the event. 
            lock = this.hzMember.getLock(lockString);
            isLocked = lock.tryLock(500L, TimeUnit.MILLISECONDS);
            if (isLocked) {
               log.info("Locked " + event.getKey().getValue());
               
               queueEvent(event.getKey());
               
            } else {
                log.info("Didn't get lock for identifier " + event.getKey().getValue());
                
            }
                  
        } catch (NullPointerException e) {
            log.debug("The event identifier was null");
            
        } catch (RuntimeException e) {
            log.debug("Couldn't get a lock for " + lockString);
            e.printStackTrace();
        
        } catch (InterruptedException e) {
            log.debug("Lock retreival was interrupted for " + lockString);
            e.printStackTrace();
          
        } finally {
            if (isLocked) {
              lock.unlock();
              log.debug("Unlocked " + lockString);
              
            }           
        }                
    }
    

    /**
     * Implement the EntryListener interface, responding to entries being evicted from
     * the hzSystemMetadata map.
     * 
     * @param event - the entry event being evicted from the map
     */
    public void entryEvicted(EntryEvent<Identifier, SystemMetadata> event) {
      // nothing to do, entry remains in backing store
      
    }

    /*
     * Queue entry added and updated event identifiers to be evaluated for
     * replication task processing
     */
    private void queueEvent(Identifier identifier) {
        boolean added = false;
        
        // add event identifiers only if they aren't already added
        log.info("The current number of potential replication events to be evaluated is: " 
            + this.replicationEvents.size());
        
        if (!this.replicationEvents.contains(identifier)) {
            added = this.replicationEvents.offer(identifier);
            if (!added) {
                log.info("Failed to add " + identifier + 
                    " to the replication event queue");
                
            }
        }                
    }

    /**
     * Get a reference to the ReplicationManager instance
     * 
     * @return replicationManager - the singleton instance of the ReplicationManager
     */
    public ReplicationManager getReplicationManager() {
        return replicationManager;
        
    }

    /**
     * Set the ReplicationManager instance
     * 
     * param replicationManager - the singleton instance of the ReplicationManager
     */
    public void setReplicationManager(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        
    }


}
