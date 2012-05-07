package org.dataone.service.cn.replication.audit;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

@DisallowConcurrentExecution
public class ReplicationAuditQuartzJob implements Job {

    private static Logger logger = Logger.getLogger(ReplicationAuditQuartzJob.class.getName());

    private ReplicationAuditService replicationAuditService = new ReplicationAuditService();

    public ReplicationAuditQuartzJob() {
    }

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.info("executing replication audit task generation job...");
        replicationAuditService.auditReplication();
    }
}