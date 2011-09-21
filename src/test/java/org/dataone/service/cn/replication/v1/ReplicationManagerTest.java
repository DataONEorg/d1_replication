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

import junit.framework.TestCase;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;
import java.util.ArrayList;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;

import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.configuration.Settings;

public class ReplicationManagerTest extends TestCase {
	
    private HazelcastInstance hzMember;
    private HazelcastInstance h1;
    private HazelcastInstance h2;

    private ReplicationManager replicationManager;

    private Config hzConfig;

    private IMap<Identifier,SystemMetadata> sysMetaMap;

    private IQueue<MNReplicationTask> replicationTasks;
    
    private IMap<NodeReference, Node> nodes;

	protected void setUp() throws Exception {
        // get reference to hazelcast.xml file and test exists
        File hzConfigFile = 
            new File("src/test/java/org/dataone/service/cn/replication/v1/hazelcast.xml");
        System.out.println("Hazelcast Config File: " + hzConfigFile.getAbsolutePath());
        System.out.println("Config File Exists: " + hzConfigFile.exists());

        // Hazelcast Config testing
        try{
            hzConfig = new FileSystemXmlConfig(hzConfigFile);
        } catch (FileNotFoundException fnfe) {
            System.err.println("FileNotFoundException: " + fnfe.getMessage());
            System.exit(1);
        }
        hzConfig.setConfigurationFile(hzConfigFile);
        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for(String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        System.out.print("Hazelcast Queues: ");
        for(String queueName : hzConfig.getQConfigs().keySet()) {
            System.out.print(queueName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.init(hzConfig);
        h1 = Hazelcast.newHazelcastInstance(hzConfig);
        h2 = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());
        System.out.println("Hazelcast member h1 name: " + h1.getName());
        System.out.println("Hazelcast member h2 name: " + h2.getName());
        Set<Member> members = hzMember.getCluster().getMembers();
        System.out.println("Cluster size " + members.size());
        for(Member m : members) {
            System.out.println(hzMember.getName() + "'s InetSocketAddress: " 
                    + m.getInetSocketAddress());
        }
		super.setUp();
	}

	protected void tearDown() throws Exception {
        Hazelcast.shutdownAll();
		super.tearDown();
	}

	/**
	 * Test that the replication service successfully becomes a Hazelcast cluster member
	 */
	public void testReplicationManager() {
        System.out.println("Testing creating a ReplicationManager");
		replicationManager = new ReplicationManager();
        //assertEquals(2, hzMember.getCluster().getMembers().size());
	}

    /**
     * Test setting replication policy on an object
	public void testSetReplicationPolicy() {
	}
     */

    /**
     * Test setting replication status on an object
	public void testSetReplicationStatus() {
	}
     */

    /**
     * Test creating and queueing tasks on a SystemMetadata change
     */
	public void testCreateAndQueueTasks() {
        // create a new SystemMetadata object for testing
        SystemMetadata sysmeta = new SystemMetadata();
        // create and set the pid 
        Identifier pid = new Identifier();
        pid.setValue("42");
        sysmeta.setIdentifier(pid);
        // create a new ReplicationPolicy
        ReplicationPolicy repPolicy = new ReplicationPolicy();
        ArrayList<NodeReference> preflist = new ArrayList<NodeReference>();
        NodeReference replica_target_1 = new NodeReference();
        NodeReference replica_target_2 = new NodeReference();
        NodeReference replica_target_3 = new NodeReference();
        replica_target_1.setValue("replica_target_1");
        replica_target_2.setValue("replica_target_2");
        replica_target_3.setValue("replica_target_3");
        preflist.add(replica_target_1);
        preflist.add(replica_target_2);
        preflist.add(replica_target_3);
        ArrayList<NodeReference> blocklist = new ArrayList<NodeReference>();
        repPolicy.setPreferredMemberNodeList(preflist);
        repPolicy.setBlockedMemberNodeList(blocklist);
        repPolicy.setReplicationAllowed(true);
        repPolicy.setNumberReplicas(3);
        NodeReference authNode = new NodeReference();
        authNode.setValue("authoritative_test_mn");
        sysmeta.setAuthoritativeMemberNode(authNode);
        // set the ReplicationPolicy for this object
        sysmeta.setReplicationPolicy(repPolicy);

        // get the name of the Hazelcast SystemMetadata IMap 
        String systemMetadataMapName =  
            Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
        String tasksQueueName = 
            Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
        String nodeMapName = 
            Settings.getConfiguration().getString("dataone.hazelcast.nodes");

        
        // get the hzSystemMetadata map 
        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        // put the object in the map
        sysMetaMap.putAsync(pid, sysmeta);

        replicationTasks = hzMember.getQueue(tasksQueueName);
        Node replica_target_1_node = new Node();
        Node replica_target_2_node = new Node();
        Node replica_target_3_node = new Node();
        replica_target_1_node.setIdentifier(replica_target_1);
        replica_target_2_node.setIdentifier(replica_target_2);
        replica_target_3_node.setIdentifier(replica_target_3);
        replica_target_1_node.setName("DataONE test replica target 1");
        replica_target_2_node.setName("DataONE test replica target 2");
        replica_target_3_node.setName("DataONE test replica target 3");
        replica_target_1_node.setDescription("A node for testing replication");
        replica_target_2_node.setDescription("A node for testing replication");
        replica_target_3_node.setDescription("A node for testing replication");
        replica_target_1_node.setBaseURL("127.0.0.1");
        replica_target_2_node.setBaseURL("127.0.0.1");
        replica_target_3_node.setBaseURL("127.0.0.1");
        replica_target_1_node.setType(NodeType.MN);
        replica_target_2_node.setType(NodeType.MN);
        replica_target_3_node.setType(NodeType.MN);
        
        nodes = hzMember.getMap(nodeMapName);
        nodes.put(replica_target_1,replica_target_1_node);
        nodes.put(replica_target_2,replica_target_2_node);
        nodes.put(replica_target_3,replica_target_3_node);
        // create the ReplicationManager
        replicationManager = new ReplicationManager();

        try{
            //replicationManager.createAndQueueTasks(pid);
            assertEquals((int) repPolicy.getNumberReplicas(), 
                    (int) replicationManager.createAndQueueTasks(pid));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Perform a query to see if the map has pending tasks
        //assertEquals((int) repPolicy.getNumberReplicas(), 
        //             (int) replicationTasks.size());
	}

}
