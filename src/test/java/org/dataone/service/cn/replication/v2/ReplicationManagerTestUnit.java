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

import java.io.InputStream;
import java.util.Set;

import javax.naming.ldap.LdapContext;

import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.integ.ServerIntegrationUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.dao.MetacatDataSourceFactory;
import org.dataone.cn.dao.ReplicationDaoMetacatImplTestUtil;
import org.dataone.cn.data.repository.ReplicationH2RepositoryFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.replication.ReplicationManager;
import org.dataone.service.cn.replication.ReplicationRepositoryFactory;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.test.apache.directory.server.integ.ApacheDSSuiteRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/org/dataone/configuration/testApplicationContext.xml" })
public class ReplicationManagerTestUnit extends AbstractLdapTestUnit {

    static Logger logger = Logger.getLogger(ReplicationManagerTestUnit.class);

    private HazelcastInstance hzMember;
    private HazelcastInstance h1;
    private HazelcastInstance h2;
    private ReplicationManager replicationManager;
    private Config hzConfig;
    private IMap<Identifier, SystemMetadata> sysMetaMap;

    private ReplicationRepositoryFactory repositoryFactory = new ReplicationH2RepositoryFactory();

    @Autowired
    @Qualifier("readSystemMetadataResource")
    private org.springframework.core.io.Resource readSystemMetadataResource;

    private JdbcTemplate jdbc = new JdbcTemplate(MetacatDataSourceFactory.getMetacatDataSource());

    @BeforeClass
    public static void beforeClass() throws Exception {
        int ldapTimeoutCount = 0;

        if (ApacheDSSuiteRunner.getLdapServer() == null) {
            throw new Exception(
                    "ApacheDSSuiteRunner was not automatically configured. FATAL ERROR!");
        }
        while (!ApacheDSSuiteRunner.getLdapServer().isStarted() && ldapTimeoutCount < 10) {
            Thread.sleep(500L);
            logger.info("LdapServer is not yet started");
            ldapTimeoutCount++;
        }
        if (!ApacheDSSuiteRunner.getLdapServer().isStarted()) {
            throw new IllegalStateException("Service is not running");
        }
        final LdapContext ctx = ServerIntegrationUtils.getWiredContext(
                ApacheDSSuiteRunner.getLdapServer(), null);
        ctx.lookup("dc=dataone,dc=org");
    }

    @Before
    public void setUp() throws Exception {
        // get reference to hazelcast.xml file and test exists
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

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
        ReplicationDaoMetacatImplTestUtil.dropTables(jdbc);
    }

    //    @Test
    public void emptyTest() {
        return;
    }

    /**
     * Because of the @Before and @After routines that assist setting up the
     * test, This single test is all we can run in this class at this time
     * 
     * Test creating and queueing tasks on a SystemMetadata change
     * @throws InterruptedException 
     */
    @Test
    public void testCreateAndQueueTasks() throws InterruptedException {
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
            InputStream is = readSystemMetadataResource.getInputStream();
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, is);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }

        replicationManager = new ReplicationManager(repositoryFactory);

        CNReplication cnReplication = new CNReplicationImpl();
        // inject a mock CNReplication Class so that we don't need a
        // CN running on a remote server somewhere in order to unit test
        replicationManager.setCnReplication(cnReplication);
        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        Thread.sleep(1500);

        try {
            int queuedTaskCount = replicationManager.createAndQueueTasks(sysmeta.getIdentifier());
            // expect numberReplicas less the already created replica
            // only 1 target node implements v2 so only 1 replica will be made (source implements v2)
            assertEquals(
                    "The number of tasks created should equal the replication policy numberOfReplicas.",
                    (int) sysmeta.getReplicationPolicy().getNumberReplicas() - 1, queuedTaskCount);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test ReplicationPolicy - Exception " + e);
        }
    }
}
