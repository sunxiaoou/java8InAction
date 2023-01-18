package lambdasinaction.tmp;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.ParseException;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class MyQuartz {
    public static void main(String[] args) throws SchedulerException, ParseException {
        // Create a scheduler instance
        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler scheduler = sf.getScheduler();

        // Create job detail
        JobDetail job = newJob(PrintWordsJob.class)
                .withIdentity("job1", "group1")
                .build();

        // Create a cron trigger
        Trigger trigger = newTrigger()
                .withIdentity("trigger1", "group1")
                // .withSchedule(cronSchedule("0 0/1 * * * ?"))
                .withSchedule(cronSchedule("0 50 10 * * ?"))
                .build();

        // Schedule job with the trigger
        scheduler.scheduleJob(job, trigger);

        // Start the scheduler
        scheduler.start();
    }
}
