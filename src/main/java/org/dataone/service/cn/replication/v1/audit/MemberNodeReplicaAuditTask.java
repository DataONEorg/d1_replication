package org.dataone.service.cn.replication.v1.audit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.dataone.service.types.v1.Identifier;

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
