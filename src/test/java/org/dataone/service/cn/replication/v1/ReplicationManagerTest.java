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

import org.dataone.service.cn.v1.CNReplication;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Set;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import javax.annotation.Resource;

import com.hazelcast.config.Config;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;

import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.configuration.Settings;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.cn.ldap.v1.NodeLdapPopulation;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/org/dataone/configuration/testApplicationContext.xml"})
public class ReplicationManagerTest {

    private HazelcastInstance hzMember;
    private HazelcastInstance h1;
    private HazelcastInstance h2;
    private ReplicationManager replicationManager;
    private Config hzConfig;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IQueue<MNReplicationTask> replicationTasks;
    private NodeLdapPopulation cnLdapPopulation;

    @Resource
    public void setCNLdapPopulation(NodeLdapPopulation ldapPopulation) {
        this.cnLdapPopulation = ldapPopulation;
    }
    @Autowired
    @Qualifier("readSystemMetadataResource")
    private org.springframework.core.io.Resource readSystemMetadataResource;
    private IMap<NodeReference, Node> nodes;

  
    @Before
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
        hzMember = Hazelcast.init(hzConfig);
        h1 = Hazelcast.newHazelcastInstance(hzConfig);
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

    }

    @After
    public void tearDown() throws Exception {
        cnLdapPopulation.deletePopulatedMns();
        Hazelcast.shutdownAll();

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
     * Because of the @Before and @After routines that assist
     * setting up the test, This single test
     * is all we can run in this class at this time
     * 
     * Test creating and queueing tasks on a SystemMetadata change
     */
    @Test
    public void testCreateAndQueueTasks() {
         assertEquals(3, hzMember.getCluster().getMembers().size());
        // get the name of the Hazelcast SystemMetadata IMap
        String systemMetadataMapName =
                Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");
        String tasksQueueName =
                Settings.getConfiguration().getString("dataone.hazelcast.replicationQueuedTasks");
        String nodeMapName =
                Settings.getConfiguration().getString("dataone.hazelcast.nodes");

        // create a new SystemMetadata object for testing
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, readSystemMetadataResource.getInputStream());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }


        /*        SystemMetadata sysmeta = new SystemMetadata();
        // create and set the pid 
        Identifier pid = new Identifier();
        pid.setValue("42");
        sysmeta.setIdentifier(pid);
        // create a new ReplicationPolicy
        ReplicationPolicy repPolicy = new ReplicationPolicy();
        ArrayList<NodeReference> preflist = new ArrayList<NodeReference>();
        NodeReference origin_reference = new NodeReference();
        NodeReference replica_target_1 = new NodeReference();
        NodeReference replica_target_2 = new NodeReference();
        NodeReference replica_target_3 = new NodeReference();
        origin_reference.setValue("sq1d");
        replica_target_1.setValue("sqR1");
        replica_target_2.setValue("sq4k");
        replica_target_3.setValue("sqrm");
        preflist.add(replica_target_1);
        preflist.add(replica_target_2);
        preflist.add(replica_target_3);
        ArrayList<NodeReference> blocklist = new ArrayList<NodeReference>();
        repPolicy.setPreferredMemberNodeList(preflist);
        repPolicy.setBlockedMemberNodeList(blocklist);
        repPolicy.setReplicationAllowed(true);
        repPolicy.setNumberReplicas(3);
        NodeReference authNode = new NodeReference();
        authNode.setValue("sq1d");
        sysmeta.setAuthoritativeMemberNode(authNode);
        // set the ReplicationPolicy for this object
        sysmeta.setReplicationPolicy(repPolicy);



        
        // get the hzSystemMetadata map 
        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        // put the object in the map
        sysMetaMap.putAsync(pid, sysmeta);

        replicationTasks = hzMember.getQueue(tasksQueueName);

        Node origin_node = new Node();
        Node replica_target_1_node = new Node();
        Node replica_target_2_node = new Node();
        Node replica_target_3_node = new Node();
        origin_node.setIdentifier(origin_reference);
        replica_target_1_node.setIdentifier(replica_target_1);
        replica_target_2_node.setIdentifier(replica_target_2);
        replica_target_3_node.setIdentifier(replica_target_3);
        origin_node.setName("DataONE test origin node");
        replica_target_1_node.setName("DataONE test replica target 1");
        replica_target_2_node.setName("DataONE test replica target 2");
        replica_target_3_node.setName("DataONE test replica target 3");
        origin_node.setDescription("A node for testing origins of replication");
        replica_target_1_node.setDescription("A node for testing replication");
        replica_target_2_node.setDescription("A node for testing replication");
        replica_target_3_node.setDescription("A node for testing replication");
        origin_node.setBaseURL("127.0.0.1");
        replica_target_1_node.setBaseURL("127.0.0.1");
        replica_target_2_node.setBaseURL("127.0.0.1");
        replica_target_3_node.setBaseURL("127.0.0.1");

        Services origin_node_services = new Services();
        Services replica_target_1_node_services = new Services();
        Services replica_target_2_node_services = new Services();
        Services replica_target_3_node_services = new Services();
        Service origin_node_repl_service = new Service();
        Service replica_target_1_rep_service = new Service();
        Service replica_target_2_rep_service = new Service();
        Service replica_target_3_rep_service = new Service();
        origin_node_repl_service.setName("MNReplication");
        replica_target_1_rep_service.setName("MNReplication");
        replica_target_2_rep_service.setName("MNReplication");
        replica_target_3_rep_service.setName("MNReplication");
        origin_node_repl_service.setVersion("v1");
        replica_target_1_rep_service.setVersion("v1");
        replica_target_2_rep_service.setVersion("v1");
        replica_target_3_rep_service.setVersion("v1");
        origin_node_repl_service.setAvailable(true);
        replica_target_1_rep_service.setAvailable(true);
        replica_target_2_rep_service.setAvailable(true);
        replica_target_3_rep_service.setAvailable(true);
        origin_node_services.addService(origin_node_repl_service);
        replica_target_1_node_services.addService(replica_target_1_rep_service);
        replica_target_2_node_services.addService(replica_target_2_rep_service);
        replica_target_3_node_services.addService(replica_target_3_rep_service);
        origin_node.setServices(origin_node_services);
        replica_target_1_node.setServices(replica_target_1_node_services);
        replica_target_2_node.setServices(replica_target_2_node_services);
        replica_target_3_node.setServices(replica_target_3_node_services);
        origin_node.setSynchronization(new Synchronization());
        replica_target_1_node.setSynchronization(new Synchronization());
        replica_target_2_node.setSynchronization(new Synchronization());
        replica_target_3_node.setSynchronization(new Synchronization());
        origin_node.setPing(new Ping());
        replica_target_1_node.setPing(new Ping());
        replica_target_2_node.setPing(new Ping());
        replica_target_3_node.setPing(new Ping()); 
        ArrayList<Subject> origin_node_subject_list = new ArrayList<Subject>();
        ArrayList<Subject> replica_target_1_node_subject_list = new ArrayList<Subject>();
        ArrayList<Subject> replica_target_2_node_subject_list = new ArrayList<Subject>();
        ArrayList<Subject> replica_target_3_node_subject_list = new ArrayList<Subject>();
        origin_node.setSubjectList(origin_node_subject_list);
        replica_target_1_node.setSubjectList(replica_target_1_node_subject_list);
        replica_target_2_node.setSubjectList(replica_target_2_node_subject_list);
        replica_target_3_node.setSubjectList(replica_target_3_node_subject_list);
        Subject subject_0 = new Subject();
        Subject subject_1 = new Subject();
        Subject subject_2 = new Subject();
        Subject subject_3 = new Subject();
        subject_0.setValue("origin_node_formal_name");
        subject_1.setValue("replica_target_node_1_formal_name");
        subject_2.setValue("replica_target_node_2_formal_name");
        subject_3.setValue("replica_target_node_3_formal_name");
        origin_node.addSubject(subject_0);
        replica_target_1_node.addSubject(subject_1);
        replica_target_2_node.addSubject(subject_2);
        replica_target_3_node.addSubject(subject_3);
        origin_node.setReplicate(true);
        replica_target_1_node.setReplicate(true);
        replica_target_2_node.setReplicate(true);
        replica_target_3_node.setReplicate(true);
        origin_node.setSynchronize(true);
        replica_target_1_node.setSynchronize(true);
        replica_target_2_node.setSynchronize(true);
        replica_target_3_node.setSynchronize(true);
        origin_node.setType(NodeType.MN);
        replica_target_1_node.setType(NodeType.MN);
        replica_target_2_node.setType(NodeType.MN);
        replica_target_3_node.setType(NodeType.MN);
        origin_node.setState(NodeState.UP);
        replica_target_1_node.setState(NodeState.UP);
        replica_target_2_node.setState(NodeState.UP);
        replica_target_3_node.setState(NodeState.UP); */


        /*        nodes.put(origin_reference, origin_node);
        nodes.put(replica_target_1,replica_target_1_node);
        nodes.put(replica_target_2,replica_target_2_node);
        nodes.put(replica_target_3,replica_target_3_node); */

        // create the ReplicationManager
        replicationManager = new ReplicationManager();
        CNReplication cnReplication = new CNReplicationImpl();
        replicationManager.setCnReplication(cnReplication);
        nodes = hzMember.getMap(nodeMapName);
        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        replicationTasks = hzMember.getQueue(tasksQueueName);

        try {
            //replicationManager.createAndQueueTasks(pid);
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
                Logger.getLogger(ReplicationManagerTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        // Perform a query to see if the map has pending tasks
        //assertEquals((int) repPolicy.getNumberReplicas(), 
        //             (int) replicationTasks.size());
    }
}
