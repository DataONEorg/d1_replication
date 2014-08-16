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
package org.dataone.service.cn.replication.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.dataone.cn.dao.DataSourceFactory;
import org.dataone.cn.dao.ReplicationDaoMetacatImplTestUtil;
import org.dataone.cn.ldap.v2.NodeLdapPopulation;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.MNReplicationTask;
import org.dataone.service.cn.replication.ReplicationFactory;
import org.dataone.service.cn.replication.ReplicationManager;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/org/dataone/configuration/testApplicationContext.xml" })
public class ReplicationManagerTest {

    private HazelcastInstance hzMember;
    private HazelcastInstance h1;
    private HazelcastInstance h2;
    private ReplicationManager replicationManager;
    private Config hzConfig;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private MultiMap<String, MNReplicationTask> replicationTaskMap;
    private NodeLdapPopulation cnLdapPopulation;

    @Resource
    public void setCNLdapPopulation(NodeLdapPopulation ldapPopulation) {
        this.cnLdapPopulation = ldapPopulation;
    }

    @Autowired
    @Qualifier("readSystemMetadataResource")
    private org.springframework.core.io.Resource readSystemMetadataResource;
    private IMap<NodeReference, Node> nodes;

    private JdbcTemplate jdbc = new JdbcTemplate(DataSourceFactory.getMetacatDataSource());

    //XX@Before
    public void setUp() throws Exception {
        // get reference to hazelcast.xml file and test exists
        cnLdapPopulation.populateTestMNs();
        // Hazelcast Config testing

        hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        System.out.print("Hazelcast Queues: ");
        for (String queueName : hzConfig.getQConfigs().keySet()) {
            System.out.print(queueName + " ");
        }
        System.out.println();
        String processingHzInstance = Settings.getConfiguration().getString(
                "dataone.hazelcast.process.instanceName");
        hzConfig.setInstanceName(processingHzInstance);
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
        hzConfig.setInstanceName(processingHzInstance + "1");
        h1 = Hazelcast.newHazelcastInstance(hzConfig);
        hzConfig.setInstanceName(processingHzInstance + "2");
        h2 = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());
        System.out.println("Hazelcast member h1 name: " + h1.getName());
        System.out.println("Hazelcast member h2 name: " + h2.getName());
        Set<Member> members = hzMember.getCluster().getMembers();
        System.out.println("Cluster size " + members.size());
        for (Member m : members) {
            System.out.println(hzMember.getName() + "'s InetSocketAddress: "
                    + m.getInetSocketAddress());
        }

        ReplicationDaoMetacatImplTestUtil.createTables(jdbc);
    }

    //XX@After
    public void tearDown() throws Exception {
        cnLdapPopulation.deletePopulatedMns();
        Hazelcast.shutdownAll();
        ReplicationDaoMetacatImplTestUtil.dropTables(jdbc);
    }

    @Test
    public void emptyTest() {
        return;
    }

    /**
     * Because of the @Before and @After routines that assist setting up the
     * test, This single test is all we can run in this class at this time
     * 
     * Test creating and queueing tasks on a SystemMetadata change
     */
    //XX@Test
    public void testCreateAndQueueTasks() {
        assertEquals(3, hzMember.getCluster().getMembers().size());
        // get the name of the Hazelcast SystemMetadata IMap
        String systemMetadataMapName = Settings.getConfiguration().getString(
                "dataone.hazelcast.systemMetadata");
        String tasksQueueName = Settings.getConfiguration().getString(
                "dataone.hazelcast.replicationQueuedTasks");
        String nodeMapName = Settings.getConfiguration().getString("dataone.hazelcast.nodes");

        // create a new SystemMetadata object for testing
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    readSystemMetadataResource.getInputStream());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }

        // create the ReplicationManager
        replicationManager = ReplicationFactory.getReplicationManager();
        CNReplication cnReplication = new CNReplicationImpl();
        // inject a mock CNReplication Class so that we don't need a
        // CN running on a remote server somewhere in order to unit test
        replicationManager.setCnReplication(cnReplication);
        nodes = hzMember.getMap(nodeMapName);
        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        replicationTaskMap = hzMember.getMultiMap("hzReplicationTaskMultiMap");

        try {
            // replicationManager.createAndQueueTasks(pid);
            // expect numberReplicas less the already created replica
            assertEquals((int) sysmeta.getReplicationPolicy().getNumberReplicas(),
                    (int) replicationManager.createAndQueueTasks(sysmeta.getIdentifier()));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test ReplicationPolicy - Exception " + e);
        }
        boolean stayAlive = true;

        for (int i = 0; i < 120; i++) {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException ex) {
                Logger.getLogger(ReplicationManagerTest.class.getName())
                        .log(Level.SEVERE, null, ex);
            }
        }
        // Perform a query to see if the map has pending tasks
        // assertEquals((int) repPolicy.getNumberReplicas(),
        // (int) replicationTasks.size());
    }
}