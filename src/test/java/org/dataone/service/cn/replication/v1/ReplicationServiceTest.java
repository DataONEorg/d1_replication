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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;

public class ReplicationServiceTest extends TestCase {
	
	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test that the replication service successfully becomes a Hazelcast cluster member
	 */
	public void testReplicationService() {

		ReplicationService replicationService = new ReplicationService();
        
	}

	public void testSetReplicationPolicy() {
		fail("Not yet implemented");
	}

	public void testSetReplicationStatus() {
		fail("Not yet implemented");
	}

	public void createAndQueueTasks() {
		fail("Not yet implemented");
	}

}
