/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
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
 * 
 */

package org.dataone.cn.data.repository;

import org.dataone.client.v1.types.D1TypeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

public class ReplicationAttemptHistoryRepositoryTest {

    private ReplicationAttemptHistoryRepository repository;
    private ReplicationAttemptHistoryH2RepositoryFactory repositoryFactory = new ReplicationAttemptHistoryH2RepositoryFactory();

    @Test
    public void testSimpleCreateReadTest() {
        int numbCreated = createAndSaveReplicationTryHistory();
        Iterable<ReplicationAttemptHistory> results = repository.findAll();
        System.out.println("Results found with findAll():");
        System.out.println("-------------------------------");
        int count = 0;
        for (ReplicationAttemptHistory result : results) {
            System.out.println(result);
            count++;
        }
        System.out.println();
        Assert.assertEquals("Total records found did not match expected.", numbCreated, count);
    }

    @Test
    public void testFindByPid() {
        createAndSaveReplicationTryHistory();
        Iterable<ReplicationAttemptHistory> results = repository.findByPid("foo_pid");
        System.out.println("Results found with findByPid('foo_pid'):");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationAttemptHistory result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("Find by pid found more records than expected", 1, count);
    }

    @Test
    public void testFindByNodeId() {
        createAndSaveReplicationTryHistory();
        Iterable<ReplicationAttemptHistory> results = repository.findByNodeId("urn:node:testNode");
        System.out.println("Results found with findByNodeId('urn:node:testNode'):");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationAttemptHistory result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("Find by nodeId did not find expected records", 2, count);
    }

    @Test
    public void testFindByPidAndNodeId() {
        createAndSaveReplicationTryHistory();
        Iterable<ReplicationAttemptHistory> results = repository.findByNodeId("urn:node:testNode");
        System.out.println("Results found with findByNodeId('urn:node:testNode'):");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationAttemptHistory result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("Find by nodeId did not find expected records", 2, count);
    }

    @Test
    public void testFindByReplicationAttempts() {
        createAndSaveReplicationTryHistory();
        Iterable<ReplicationAttemptHistory> results = repository.findByPidAndNodeId("bar_pid",
                "urn:node:testNode");
        System.out
                .println("Results found with findByPidAndNodeId(\"bar_pid\", \"urn:node:testNode\"):");
        System.out.println("--------------------------------------------");
        int count = 0;
        for (ReplicationAttemptHistory result : results) {
            System.out.println(result);
            count++;
        }
        Assert.assertEquals("Find by PidAndNodeId did not find expected records", 1, count);
    }

    @Test
    public void testPagingQuery() {
        repository.deleteAll();

        repository.save(new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("foo_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(4)));

        repository.save(new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("bar_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(14)));

        repository.save(new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("foo_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode2"), Integer.valueOf(2)));

        repository.save(new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("bar_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode2"), Integer.valueOf(1)));

        repository.save(new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("foobar_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(12)));

        int pageSize = 1;
        int startPageNumber = 0;

        PageRequest pageRequest = new PageRequest(startPageNumber, pageSize, new Sort(
                Sort.Direction.ASC, "id"));
        Page<ReplicationAttemptHistory> pagedResults = repository.findAll(pageRequest);

        Assert.assertEquals("Total elements does not match expected", 5,
                pagedResults.getTotalElements());
        Assert.assertEquals("Total pages does not match expected", 5, pagedResults.getTotalPages());
        Assert.assertTrue("First Page does not agree", pagedResults.isFirstPage());
        Assert.assertFalse("Last Page does not agree", pagedResults.isLastPage());
        Assert.assertEquals("Page size is wrong", 1, pagedResults.getContent().size());

        System.out.println("page " + pagedResults.getNumber() + ": "
                + pagedResults.getContent().get(0));
        while (pagedResults.hasNextPage()) {
            pagedResults = repository.findAll(pagedResults.nextPageable());
            Assert.assertEquals("Page size is wrong", 1, pagedResults.getContent().size());
            System.out.println("page " + pagedResults.getNumber() + ": "
                    + pagedResults.getContent().get(0));
        }
    }

    @Test
    public void testIncrementAttempts() {
        ReplicationAttemptHistory tryHistory = new ReplicationAttemptHistory(
                D1TypeBuilder.buildIdentifier("foo_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(4));
        tryHistory.incrementReplicationAttempts();
        Assert.assertEquals("Increment did not work properly.", Integer.valueOf(5),
                tryHistory.getReplicationAttempts());
    }

    private int createAndSaveReplicationTryHistory() {
        repository.deleteAll();
        ReplicationAttemptHistory tryHistory = new ReplicationAttemptHistory(
                D1TypeBuilder.buildIdentifier("foo_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(4));
        repository.save(tryHistory);
        tryHistory = new ReplicationAttemptHistory(D1TypeBuilder.buildIdentifier("bar_pid"),
                D1TypeBuilder.buildNodeReference("urn:node:testNode"), Integer.valueOf(2));
        repository.save(tryHistory);
        return 2;
    }

    @Before
    public void setUp() {
        repository = repositoryFactory.getReplicationTryHistoryRepository();
    }

    @After
    public void tearDown() {
        repositoryFactory.closeContext();
    }
}
