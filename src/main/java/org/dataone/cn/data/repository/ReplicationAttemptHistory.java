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

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.hibernate.annotations.Index;

@Entity
@Table(name = "replication_try_history")
public class ReplicationAttemptHistory implements Serializable {

    private static final long serialVersionUID = -3273504291943429461L;

    @Transient
    private static final Log log = LogFactory.getLog(ReplicationAttemptHistory.class);

    @Transient
    private final FastDateFormat format = FastDateFormat.getInstance("MM/dd/yyyy:HH:mm:ss:SS");

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Index(name = "index_pid")
    @Column(columnDefinition = "TEXT", nullable = false)
    private String pid;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String nodeId;

    private Integer replicationAttempts = 0;

    @Column(nullable = false)
    private long lastReplicationAttemptDate;

    // default constructor required by jpa
    public ReplicationAttemptHistory() {
    }

    public ReplicationAttemptHistory(Identifier identifier, NodeReference nodeReference,
            Integer numbReplicationAttempts) {
        if (identifier != null) {
            pid = identifier.getValue();
        }
        if (nodeReference != null) {
            nodeId = nodeReference.getValue();
        }
        if (numbReplicationAttempts != null) {
            replicationAttempts = numbReplicationAttempts;
        }
        this.lastReplicationAttemptDate = System.currentTimeMillis();
    }

    @Transient
    public String getLastReplicationAttemptDateString() {
        return format.format(this.getLastReplicationAttemptDate());
    }

    public Long getId() {
        return id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Integer getReplicationAttempts() {
        return replicationAttempts;
    }

    public void setReplicationAttempts(Integer replicationAttempts) {
        this.replicationAttempts = replicationAttempts;
    }

    public long getLastReplicationAttemptDate() {
        return lastReplicationAttemptDate;
    }

    public void setLastReplicationAttemptDate(long lastReplicationAttemptDate) {
        this.lastReplicationAttemptDate = lastReplicationAttemptDate;
    }

    @Transient
    public void incrementReplicationAttempts() {
        setReplicationAttempts(Integer.valueOf(this.replicationAttempts.intValue() + 1));
    }

    @Override
    @Transient
    public String toString() {
        return "ReplicationTryHistory [id=" + id + ", pid=" + pid + ", nodeId=" + getNodeId()
                + ", replication attempts=" + getReplicationAttempts() + ", last attempt="
                + getLastReplicationAttemptDateString() + "']";
    }
}
