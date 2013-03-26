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
package org.dataone.service.cn.replication.v1.audit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.dataone.service.types.v1.Identifier;

/**
 * Callable java task, delegates to MemberNodeReplicaAuditingStrategy to handle
 * audit work for each pid in pids.
 * 
 * @author sroseboo
 *
 */
public class MemberNodeReplicaAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = 8549092026722882706L;

    private List<Identifier> pidsToAudit = new ArrayList<Identifier>();
    private MemberNodeReplicaAuditingStrategy auditor;
    private Date auditDate;

    public MemberNodeReplicaAuditTask(List<Identifier> pids, Date auditDate) {
        this.pidsToAudit = pids;
        this.auditDate = auditDate;
        auditor = new MemberNodeReplicaAuditingStrategy();
    }

    @Override
    public String call() throws Exception {
        auditor.auditPids(pidsToAudit, auditDate);
        return "Replica audit task for pids: " + pidsToAudit + " completed.";
    }

    public List<Identifier> getPidsToAudit() {
        return pidsToAudit;
    }
}
