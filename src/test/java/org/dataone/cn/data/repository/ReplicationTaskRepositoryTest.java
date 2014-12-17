package org.dataone.cn.data.repository;

import java.util.List;

import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.service.cn.replication.ReplicationRepositoryFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

public class ReplicationTaskRepositoryTest {

    private ReplicationTaskRepository repository;
    private ReplicationRepositoryFactory repositoryFactory = new ReplicationH2RepositoryFactory();

    @Test
    public void testSimpleCreateReadTest() {
        repository.deleteAll();
        int numbCreated = createAndSaveReplicationTask();
        Iterable<ReplicationTask> results = repository.findAll();
        System.out.println("Results found with findAll():");
        System.out.println("-------------------------------");
        int count = 0;
        for (ReplicationTask result : results) {
            System.out.println(result);
            count++;
        }
        System.out.println();
        Assert.assertEquals("Total records found did not match expected.", numbCreated, count);
    }

    @Test
    public void testExecutionBackoff() {
        repository.deleteAll();
        createAndSaveReplicationTask();
        List<ReplicationTask> results = repository.findByPid("foo_pid");
        for (ReplicationTask task : results) {
            task.markInProcess();
            repository.save(task);
        }
        results = repository.findByPid("foo_pid");
        for (ReplicationTask task : results) {
            if (task.isProcessing()) {
                task.markNew();
                repository.save(task);
            }
        }
        long now = System.currentTimeMillis();
        results = repository.findByStatusAndNextExecutionLessThan(ReplicationTask.STATUS_NEW, now);
        Assert.assertEquals("did not find expected tasks for execution", 1, results.size());

        results = repository.findByPid("foo_pid");
        for (ReplicationTask task : results) {
            task.markInProcess();
            task.markNew();
            repository.save(task);
        }
        results = repository.findByStatusAndNextExecutionLessThan(ReplicationTask.STATUS_NEW, now);
        Assert.assertEquals("did not find expected tasks for execution", 1, results.size());
    }

    @Test
    public void testFindByPid() {
        repository.deleteAll();
        createAndSaveReplicationTask();
        Iterable<ReplicationTask> results = repository.findByPid("foo_pid");
        System.out.println("Results found with findByPid('foo_pid'):");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationTask result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("Find by pid found more records than expected", 1, count);
    }

    @Test
    public void testFindByStatusAndNextExecutionPagingWithSort() {
        repository.deleteAll();
        createAndSaveReplicationTask();

        ReplicationTask task = new ReplicationTask(D1TypeBuilder.buildIdentifier("new_pid"));
        task.setNextExecution(100);
        repository.save(task);

        Pageable page = new PageRequest(0, 1);
        Iterable<ReplicationTask> results = repository
                .findByStatusAndNextExecutionLessThanOrderByNextExecutionAsc(
                        ReplicationTask.STATUS_NEW, System.currentTimeMillis(), page);
        System.out.println("Results found with findbyStatusAndNextExecution:");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationTask result : results) {
            System.out.println(result);
            count++;
            Assert.assertEquals(
                    "page 1 of findByStatusAndNextExecution with sort did not find expected pid",
                    "new_pid", result.getPid());
        }
        Assert.assertEquals("findbyStatusAndNextExecution found more records than expected", 1,
                count);

        page = new PageRequest(0, 20);
        results = repository.findByStatusAndNextExecutionLessThanOrderByNextExecutionAsc(
                ReplicationTask.STATUS_NEW, System.currentTimeMillis(), page);
        System.out.println("Results found with findbyStatusAndNextExecution:");
        System.out.println("--------------------------------------------");
        count = 0;
        for (ReplicationTask result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("findbyStatusAndNextExecution found more records than expected", 3,
                count);
    }

    private int createAndSaveReplicationTask() {
        ReplicationTask task = new ReplicationTask(D1TypeBuilder.buildIdentifier("foo_pid"));
        repository.save(task);
        task = new ReplicationTask(D1TypeBuilder.buildIdentifier("bar_pid"));
        repository.save(task);
        return 2;
    }

    @Before
    public void setUp() {
        repositoryFactory.initContext();
        repository = repositoryFactory.getReplicationTaskRepository();
    }

    @After
    public void tearDown() {
        repositoryFactory.closeContext();
    }
}
