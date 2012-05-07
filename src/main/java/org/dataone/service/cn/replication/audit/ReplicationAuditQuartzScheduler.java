package org.dataone.service.cn.replication.audit;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;

public class ReplicationAuditQuartzScheduler {

    private static Log logger = LogFactory.getLog(ReplicationAuditQuartzScheduler.class.getName());

    private static final String QUARTZ_TRIGGER = "replica-audit-trigger";
    private static final String QUARTZ_GROUP = "d1-cn-replica-audit";
    private static final String QUARTZ_JOB = "d1-replica-audit-job";

    private int hours = 24;

    private Scheduler scheduler;

    public void start() {
        try {
            logger.info("starting replica audit task generation quartz scheduler....");
            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream(
                    "/org/dataone/configuration/quartz.properties"));
            SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();

            JobDetail job = newJob(ReplicationAuditQuartzJob.class).withIdentity(QUARTZ_JOB,
                    QUARTZ_GROUP).build();

            // TODO: NEED TO SCHEDULE BY TIME OF DAY TO SYNCH WITH OTHER CN
            SimpleTrigger trigger = newTrigger().withIdentity(QUARTZ_TRIGGER, QUARTZ_GROUP)//
                    .startNow() //
                    .withSchedule(SimpleScheduleBuilder.repeatHourlyForever(hours)) //
                    .build();

            scheduler.scheduleJob(job, trigger);
            scheduler.start();

        } catch (SchedulerException e) {
            logger.error("Replication Audit Scheduler start() ", e);
        } catch (FileNotFoundException e) {
            logger.error("Replication Audit Scheduler start() ", e);
        } catch (IOException e) {
            logger.error("Replication Audit Scheduler start() ", e);
        }
    }

    public void stop() {
        logger.info("stopping replica audit task generator quartz scheduler...");
        try {
            if (scheduler.isStarted()) {
                scheduler.standby();
                while (!(scheduler.getCurrentlyExecutingJobs().isEmpty())) {
                    logger.info("Job executing, waiting for it to complete.....");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        logger.warn("Sleep interrupted. check again!");
                    }
                }
                scheduler.deleteJob(jobKey(QUARTZ_JOB, QUARTZ_GROUP));
            }
        } catch (SchedulerException e) {
            logger.error("Replication Aduit Scheduler stop() ", e);
        }
    }
}
