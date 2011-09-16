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

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

public class ReplicationManagerTest extends TestCase {
	
    private HazelcastInstance hzMember;

    private Config hzConfig;

	protected void setUp() throws Exception {

		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test that the replication service successfully becomes a Hazelcast cluster member
	 */
	public void testReplicationManager() {
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
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(hzConfig);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());
        System.out.println("Hazelcast member h1 name: " + h1.getName());
        System.out.println("Hazelcast member h2 name: " + h2.getName());
        Set<Member> members = hzMember.getCluster().getMembers();
        System.out.println("Cluster size " + members.size());
        for(Member m : members) {
            System.out.println(hzMember.getName() + "'s InetSocketAddress: " 
                    + m.getInetSocketAddress());
        }


        System.out.println("Testing creating a ReplicationManager");
		ReplicationManager replicationManager = new ReplicationManager();

	}

	public void testSetReplicationPolicy() {
	}

	public void testSetReplicationStatus() {
	}

	public void testCreateAndQueueTasks() {
	}

}
