package org.dataone.service.cn.replication.v1;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.dataone.cn.dao.MetacatDataSourceFactory;
import org.dataone.cn.dao.ReplicationDaoMetacatImplTestUtil;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class TestReplicationPrioritization {

    private ReplicationPrioritizationStrategy prioritiyStrategy = new ReplicationPrioritizationStrategy();

    private JdbcTemplate jdbc = new JdbcTemplate(
            MetacatDataSourceFactory.getMetacatDataSource());

    @Test
    public void testRequestFactorCalculation() throws Exception {

        // Statuses: COMPLETE, REQUESTED, QUEUED, FAILED, INVALIDATED

        // 11 Pending requests for node1 should max it out.
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "QUEUED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "QUEUED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");

        // noise to make sure other nodes remain open with less than 10 AND with
        // records that aren't 'pending'
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "COMPLETED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid1", "node2", "FAILED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node2", "QUEUED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "COMPLETED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "FAILED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node2", "COMPLETED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node2", "FAILED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "COMPLETED", "2010-01-01 12:00:00");

        // node 3 - 2 queued tasks - below threshold
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "QUEUED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node3", "QUEUED", "2010-01-01 12:00:00");

        // node 4 is boostrap test - no data

        // nodelist : node1, node2, node3, node4
        List<NodeReference> nodeIds = createNodeList();

        Map<NodeReference, Float> requestFactors = prioritiyStrategy
                .getPendingRequestFactors(nodeIds, false);
        for (NodeReference nodeRef : requestFactors.keySet()) {
            System.out.println("Node: " + nodeRef.getValue() + " request factor: "
                    + requestFactors.get(nodeRef));
            if ("node1".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 0.0f);
            } else if ("node2".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 1.0f);
            } else if ("node3".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 1.0f);
            } else if ("node4".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 1.0f);
            }
        }
    }

    @Test
    public void testFailureFactor() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(System.currentTimeMillis()));
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 2);

        // node1 - all success and noise
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "INVALIDATED", cal.getTime());

        // node2 - 2 failures, 4 completed -
        // .666 success factor below threshold of .8
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", cal.getTime());

        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node2", "COMPLETED", cal.getTime());

        // node3 - 1 failure - 5 completed - above threshold.
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "FAILED", cal.getTime());

        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node3", "COMPLETED", cal.getTime());

        // node4 is bootstrap test - no data.

        // nodelist : node1, node2, node3, node4
        List<NodeReference> nodeIds = createNodeList();

        Map<NodeReference, Float> requestFactors = prioritiyStrategy
                .getFailureFactors(nodeIds, false);
        for (NodeReference nodeRef : requestFactors.keySet()) {
            System.out.println("Node: " + nodeRef.getValue() + " request factor: "
                    + requestFactors.get(nodeRef));
            if ("node1".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 1.0f);
            } else if ("node2".equals(nodeRef.getValue())) {
                Assert.assertTrue(requestFactors.get(nodeRef) == 0.0f);
            } else if ("node3".equals(nodeRef.getValue())) {
                Assert.assertEquals(0.8333333f, requestFactors.get(nodeRef)
                        .floatValue());
            } else if ("node4".equals(nodeRef.getValue())) {
                Assert.assertEquals(requestFactors.get(nodeRef), 1.0f);
            }
        }
    }

    @Test
    public void testNodePriorization() {

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(System.currentTimeMillis()));
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 2);

        // node1 will be only node that makes it. few queued and few failures
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "INVALIDATED", cal.getTime());

        // node2 is the blocked node - this is really just noise
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid1", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node2", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node2", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node2", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node2", "COMPLETED", cal.getTime());

        // node3 - 1 failure - 5 completed - above threshold but will be 'full'
        // with queued
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node3", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node3", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node3", "QUEUED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node3", "REQUESTED", cal.getTime());

        // node 4 will score 0 due to many failures even though it is preferred
        // member node
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node4", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node4", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node4", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node4", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node4", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node4", "COMPLETED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node4", "FAILED", cal.getTime());
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node4", "COMPLETED", cal.getTime());

        List<NodeReference> nodeIds = createNodeList();
        // need system metadata object with blocked and preferred member nodes.
        List<NodeReference> preferredNodes = new ArrayList<NodeReference>();
        List<NodeReference> blockedNodes = new ArrayList<NodeReference>();
        for (NodeReference node : nodeIds) {
            if ("node4".equals(node.getValue())) {
                preferredNodes.add(node);
            } else if ("node2".equals(node.getValue())) {
                blockedNodes.add(node);
            }
        }

        SystemMetadata sysmeta = createSystemMetadata("testPid1", preferredNodes,
                blockedNodes);
        List<NodeReference> nodes = prioritiyStrategy.prioritizeNodes(nodeIds,
                sysmeta);
        for (NodeReference node : nodes) {
            System.out.println("Node: " + node.getValue());
        }
        // node1 is preferred with open room in queued and above failure
        // threshold.
        // node 2 is a blocked node so not used, no matter failure, requested
        // metrics
        // node3 is full, too many queued
        // node4 has too many failures: 
        
        //updated: not using failure factor at the moment, node4 succeeds
        //Assert.assertTrue(nodes.size() == 1);
        Assert.assertTrue(nodes.size() == 2);
    }

    @Before
    public void setUp() {
        ReplicationDaoMetacatImplTestUtil.createTables(jdbc);
    }

    @After
    public void tearDown() {
        ReplicationDaoMetacatImplTestUtil.dropTables(jdbc);
    }

    private SystemMetadata createSystemMetadata(String pidValue,
            List<NodeReference> preferredNodes, List<NodeReference> blockedNodes) {
        SystemMetadata systemMetadata = new SystemMetadata();

        Identifier identifier = new Identifier();
        identifier.setValue(pidValue);
        systemMetadata.setIdentifier(identifier);

        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue("eml2.0.1");
        systemMetadata.setFormatId(fmtid);

        systemMetadata.setSerialVersion(BigInteger.TEN);
        systemMetadata.setSize(BigInteger.TEN);
        Checksum checksum = new Checksum();
        checksum.setValue("V29ybGQgSGVsbG8h");
        checksum.setAlgorithm("SHA-1");
        systemMetadata.setChecksum(checksum);

        Subject rightsHolder = new Subject();
        rightsHolder.setValue("DataONE");
        systemMetadata.setRightsHolder(rightsHolder);

        Subject submitter = new Subject();
        submitter.setValue("Kermit de Frog");
        systemMetadata.setSubmitter(submitter);

        systemMetadata.setDateSysMetadataModified(new Date());

        ReplicationPolicy replicationPolicy = new ReplicationPolicy();
        replicationPolicy.setNumberReplicas(4);
        replicationPolicy.setReplicationAllowed(true);
        replicationPolicy.setBlockedMemberNodeList(blockedNodes);
        replicationPolicy.setPreferredMemberNodeList(preferredNodes);

        systemMetadata.setReplicationPolicy(replicationPolicy);

        return systemMetadata;
    }

    private List<NodeReference> createNodeList() {
        List<NodeReference> nodeIds = new ArrayList<NodeReference>();
        NodeReference node1 = new NodeReference();
        node1.setValue("node1");
        nodeIds.add(node1);

        NodeReference node2 = new NodeReference();
        node2.setValue("node2");
        nodeIds.add(node2);

        NodeReference node3 = new NodeReference();
        node3.setValue("node3");
        nodeIds.add(node3);

        NodeReference node4 = new NodeReference();
        node4.setValue("node4");
        nodeIds.add(node4);

        return nodeIds;
    }
}
