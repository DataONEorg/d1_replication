package org.dataone.cn.data.repository;

import java.io.Serializable;
import java.util.Calendar;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.dataone.service.types.v1.Identifier;
import org.hibernate.annotations.Index;

@Entity
@Table(name = "replication_task_queue")
public class ReplicationTask implements Serializable {

    @Transient
    private static final long serialVersionUID = -7805808933644298181L;

    @Transient
    private static final Logger log = Logger.getLogger(ReplicationTask.class);

    @Transient
    private final FastDateFormat format = FastDateFormat.getInstance("MM/dd/yyyy:HH:mm:ss:SS");

    @Transient
    private static final int ALLOWED_RETRIES = 2;

    @Transient
    public static final String STATUS_NEW = "NEW";

    @Transient
    public static final String STATUS_IN_PROCESS = "IN PROCESS";

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Index(name = "index_pid_task")
    @Column(columnDefinition = "TEXT", nullable = false)
    private String pid;

    @Column(nullable = false)
    private long nextExecution = 0;

    @Column(nullable = false)
    private int tryCount = 0;

    @Column(nullable = false)
    private String status;

    public ReplicationTask() {
        markNew();
    }

    public ReplicationTask(Identifier identifier) {
        this();
        if (identifier != null) {
            pid = identifier.getValue();
        }
    }

    @Transient
    public String getNextExecutionDateString() {
        return format.format(this.getNextExecution());
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

    public long getNextExecution() {
        return this.nextExecution;
    }

    public void setNextExecution(long next) {
        this.nextExecution = next;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int count) {
        this.tryCount = count;
    }

    public String getStatus() {
        return status;
    }

    // Do not use this method, used by unit tests only.
    // use the specific 'markNew, markFailed, markInProcess' methods.
    public void setStatus(String status) {
        if (status != null) {
            this.status = status;
        }
    }

    @Transient
    public void markNew() {
        this.setStatus(STATUS_NEW);
        setNextExecutionTimeWithBackoff();

    }

    @Transient
    public void markInProcess() {
        this.setStatus(STATUS_IN_PROCESS);
        this.tryCount++;
    }

    @Transient
    public boolean isProcessing() {
        boolean isProcessing = false;
        if (STATUS_IN_PROCESS.equals(this.status)) {
            isProcessing = true;
        }
        return isProcessing;
    }

    @Transient
    private void setNextExecutionTimeWithBackoff() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        int tryCount = getTryCount();
        if (tryCount < ALLOWED_RETRIES) {
            setNextExecution(cal.getTimeInMillis());
        } else if (tryCount == ALLOWED_RETRIES) {
            cal.add(Calendar.MINUTE, 20);
            setNextExecution(cal.getTimeInMillis());
        } else if (tryCount == ALLOWED_RETRIES + 1) {
            cal.add(Calendar.HOUR, 2);
            setNextExecution(cal.getTimeInMillis());
        } else if (tryCount == ALLOWED_RETRIES + 2) {
            cal.add(Calendar.HOUR, 8);
            setNextExecution(cal.getTimeInMillis());
        } else if (tryCount >= ALLOWED_RETRIES + 3 && tryCount <= ALLOWED_RETRIES + 5) {
            cal.add(Calendar.HOUR, 24);
            setNextExecution(cal.getTimeInMillis());
        } else if (tryCount > ALLOWED_RETRIES + 5) {
            cal.add(Calendar.DATE, 7);
            setNextExecution(cal.getTimeInMillis());
        }
    }

    @Override
    public String toString() {
        return "ReplicationTask [id=" + id + ", pid=" + pid + ", status=" + status + ", tryCount="
                + tryCount + ", nextExecution=" + getNextExecutionDateString() + "]";
    }
}
