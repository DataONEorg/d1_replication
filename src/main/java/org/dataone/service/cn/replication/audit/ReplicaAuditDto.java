package org.dataone.service.cn.replication.audit;

import java.util.ArrayList;
import java.util.List;

public class ReplicaAuditDto {

    private String pid;
    private List<String> nodeReferences = new ArrayList<String>();
    private int numberOfReplicas; // check that all replicas have been created.

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public List<String> getNodeReferences() {
        return nodeReferences;
    }

    public void setNodeReferences(List<String> nodeReferences) {
        this.nodeReferences = nodeReferences;
    }
}
