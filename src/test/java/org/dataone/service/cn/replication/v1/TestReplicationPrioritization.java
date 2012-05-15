package org.dataone.service.cn.replication.v1;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.DataSourceFactory;
import org.dataone.cn.dao.ReplicationDao;
import org.dataone.cn.dao.ReplicationDaoMetacatImplTestUtil;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class TestReplicationPrioritization {

    private ReplicationDao replicationDao = DaoFactory.getReplicationDao();

    private ReplicationPrioritizationStrategy prioritiyStrategy = new ReplicationPrioritizationStrategy();

    private JdbcTemplate jdbc = new JdbcTemplate(
            DataSourceFactory.getMetacatDataSource());

    @Test
    public void testPrioritization() throws Exception {

        // Statuses: COMPLETE, REQUESTED, QUEUED, FAILED, INVALIDATED
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid2", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid3", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid4", "node1", "REQUESTED", "2010-01-01 12:00:00");
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid5", "node1", "REQUESTED", "2010-01-01 12:00:00");

        // nodelist : node1, node2, node3
        List<NodeReference> nodeIds = createNodeList();

        List<NodeReference> nodeIdentifiers;
        Map<NodeReference, Float> requestFactors = prioritiyStrategy
                .getPendingRequestFactors(nodeIds, false);
        for (NodeReference nodeRef : requestFactors.keySet()) {
            System.out.println("Node: " + nodeRef.getValue()
                    + " request factor: " + requestFactors.get(nodeRef));
        }

    }

    @Test
    public void testPrioritization2() throws Exception {
        ReplicationDaoMetacatImplTestUtil.createReplicationStatusRecord(jdbc,
                "test_guid", "node1", "REQUESTED", "2010-01-01 12:00:00");

        List<Identifier> results = replicationDao.getReplicasByDate(new Date(
                System.currentTimeMillis()), 0, 0);
        Assert.assertTrue(results.size() == 1);
    }

    @Before
    public void setUp() {
        System.out.println("set up");
        ReplicationDaoMetacatImplTestUtil.createTables(jdbc);
    }

    @After
    public void tearDown() {
        System.out.println("tear down");
        ReplicationDaoMetacatImplTestUtil.dropTables(jdbc);
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

        return nodeIds;
    }
}
