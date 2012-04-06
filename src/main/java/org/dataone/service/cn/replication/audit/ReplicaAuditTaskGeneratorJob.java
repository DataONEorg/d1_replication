package org.dataone.service.cn.replication.audit;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@DisallowConcurrentExecution
public class ReplicaAuditTaskGeneratorJob implements Job {

    private static Logger logger = Logger.getLogger(ReplicaAuditTaskGeneratorJob.class.getName());

    private static ApplicationContext context;
    private static ReplicaAuditTaskGenerator generator;

    public ReplicaAuditTaskGeneratorJob() {
    }

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.info("executing replication audit task generation job...");
        setContext();
        generator.generateAuditTasks();
    }

    private static void setContext() {
        if (context == null || generator == null) {
            context = new ClassPathXmlApplicationContext("replication-audit-context.xml");
            generator = (ReplicaAuditTaskGenerator) context.getBean("replicaAuditTaskGenerator");
        }
    }
}