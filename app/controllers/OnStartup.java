package controllers;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlRow;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Singleton
public class OnStartup {

    Config conf = ConfigFactory.load("application.conf");
    String serviceName=conf.getString("service.name");

    @Inject
    public OnStartup() {
        try {

            SchedulerFactory schfa = new StdSchedulerFactory();
            Scheduler scheduler = schfa.getScheduler();
            String updateTracker = "update  bodhee.jdbc_job_tracker set status = 'START';commit";
            Ebean.createSqlUpdate(updateTracker).execute();

            String jobSchedulerSql = "select * from bodhee.jdbc_job_scheduler where active = true and service_name='"+serviceName+"'";
            List<SqlRow> jobSchedulerData = Ebean.createSqlQuery(jobSchedulerSql).findList();
            ScheduleJobsController object = new ScheduleJobsController();

            if (jobSchedulerData.size() > 0) {
                ExecutorService executorService = Executors.newFixedThreadPool(jobSchedulerData.size());
                //execute accept runnable object we can do that with a lambda function.
                jobSchedulerData.stream().forEach(sqlRow -> {
                    executorService.execute(() -> {
                        try {
                            object.doCopyJob(sqlRow,scheduler);
                            Thread.sleep(2L * 1000L);

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                });

            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}