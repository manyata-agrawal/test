package controllers;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import play.Logger;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.Date;
import java.util.UUID;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

class ScheduleJobsController extends Controller {


    public /*synchronized*/ void doCopyJob(SqlRow sqlRow, Scheduler scheduler) throws InterruptedException {
        Long schJobId = 0l;
        try {
            //SchedulerFactory sch = new StdSchedulerFactory();
            //Scheduler scheduler = sch.getScheduler();
            Logger.info(" Copy job scheduler started " + new Date() + " - " + sqlRow.get("job_name"));
            String cron = sqlRow.getString("cron");
            JobDataMap jobDataMap = new JobDataMap();
            //DataProcessingJobs pobj = new DataProcessingJobs(jobs.getTenant(), jobs.getTopicName(),true,jobs.getCron(),jobs.getActionFlag());
            Long jId = sqlRow.get("jdbc_connector_id") == null ? 0 : sqlRow.getLong("jdbc_connector_id");
            schJobId = sqlRow.get("id") == null ? 0 : sqlRow.getLong("id");

            jobDataMap.put("jobId", jId);
            //DataProcessingJobHistory hObj = DataProcessingJobHistory.findByProcessingJobId(jobs.id);

            JobDetail job = newJob(ConnectorController.class).withIdentity(UUID.randomUUID().toString()).setJobData(jobDataMap)
                    .build();
            Trigger jobTrigger = newTrigger().withIdentity(UUID.randomUUID().toString())
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
            scheduler.scheduleJob(job, jobTrigger);
            scheduler.start();

            Logger.info("Updating bodhee.jdbc_job_scheduler "+ new Date());

            String updateJobData = "update bodhee.jdbc_job_scheduler set scheduler_id = '" + job.getKey().toString() + "' ,trigger_id = '" + jobTrigger.getKey().toString() + "'" +
                    " ,last_update_date = current_timestamp where id = " + schJobId + ";commit";

            Logger.info("Running the query : "+updateJobData+" "+ new Date());


            Ebean.createSqlUpdate(updateJobData).execute();


            Logger.info(" bodhee.jdbc_job_scheduler is updated "+ new Date());

        } catch (SchedulerException e) {
            String updateJobData = "update bodhee.jdbc_job_scheduler set status = 'FAILED' where id = " + schJobId + ";commit";
            Ebean.createSqlUpdate(updateJobData).execute();

            e.printStackTrace();
            Logger.debug("trace ", e);
            Logger.error("exception " + e);
        }
        Logger.info(" Copy job scheduler ended " + new Date() + " - " + sqlRow.get("job_name"));
    }


}
