package org.dataone.service.cn.replication.v1.audit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.dataone.service.types.v1.Identifier;

public class MemberNodeReplicaAuditTask implements Serializable, Callable<String> {

    private static final long serialVersionUID = 8549092026722882706L;

    private String taskid;
    private List<Identifier> pidsToAudit = new ArrayList<Identifier>();
    private MemberNodeReplicaAuditingStrategy auditor;

    public MemberNodeReplicaAuditTask(String taskid, List<Identifier> pids) {
        this.taskid = taskid;
        this.pidsToAudit = pids;
        auditor = new MemberNodeReplicaAuditingStrategy();
    }

    @Override
    public String call() throws Exception {
        auditor.auditPids(pidsToAudit);
        return "Replica audit task: " + taskid + " for pids: " + pidsToAudit + " completed.";
    }

    public List<Identifier> getPidsToAudit() {
        return pidsToAudit;
    }
}
