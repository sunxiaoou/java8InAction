package lambdasinaction.tmp;

import com.alibaba.fastjson.JSONObject;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Quartz {
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
    private final Scheduler scheduler;

    public Quartz() {
        try {
            Properties p = new Properties();
            p.setProperty("org.quartz.threadPool.threadCount", "3");
            p.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");

            SchedulerFactory sf = new StdSchedulerFactory(p);
            this.scheduler = sf.getScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void addJob(String name, String group, String cronStr, Class<? extends Job> jobClass) {
        try {
            JobDetail job = newJob(jobClass)
                    .withIdentity(name, group)
                    .build();
            Trigger trigger = newTrigger()
                    .withIdentity("t_" + name, "t_" + group)
                    .withSchedule(cronSchedule(cronStr))
                    .build();
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean deleteJobs(String group) {
        try {
            Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.groupEquals(group));
            return scheduler.deleteJobs(new ArrayList<>(jobKeys));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        try {
            scheduler.start();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void showJobKeys(String group) {
        try {
            Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.groupEquals(group));
            jobKeys.forEach(System.out::println);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public static JSONObject cronStrToJson(String cronStr) {
        JSONObject jsonObject = new JSONObject();
        List<String> cronParts = Arrays.asList(cronStr.split("\\s+"));
        if (cronParts.size() > 5) {
            jsonObject.put("seconds", cronParts.get(0));
            cronParts = cronParts.subList(1, 6);
        }
        jsonObject.put("minutes", cronParts.get(0));
        jsonObject.put("hours", cronParts.get(1));
        jsonObject.put("dayOfMonth", cronParts.get(2));
        jsonObject.put("month", cronParts.get(3));
        jsonObject.put("dayOfWeek", cronParts.get(4));
        return jsonObject;
    }

    public static String jsonToCornStr(JSONObject cronJson) {
        return (cronJson.containsKey("seconds") ? cronJson.getString("seconds") : "0") + " " +
                (cronJson.containsKey("minutes") ? cronJson.getString("minutes") : "*") + " " +
                (cronJson.containsKey("hours") ? cronJson.getString("hours") : "*") + " " +
                (cronJson.containsKey("dayOfMonth") ? cronJson.getString("dayOfMonth") : "*") + " " +
                (cronJson.containsKey("month") ? cronJson.getString("month") : "*") + " " +
                (cronJson.containsKey("dayOfWeek") ? cronJson.getString("dayOfWeek") : "?");
    }

    public static void main(String[] args) {
        // String cronStr = "0 31 17 * * ?";
        // String cronStr = "0 0/1 * * * ?";
        String cronStr = "0/5 * * * * ?";
        JSONObject cronJson = Quartz.cronStrToJson(cronStr);
        System.out.println(Quartz.cronStrToJson(cronStr).toString());
        System.out.println(Quartz.jsonToCornStr(cronJson));
        try {
            Quartz quartz = new Quartz();
            quartz.addJob(String.valueOf(1), "JobA", cronStr, JobA.class);
            quartz.start();
            System.out.println("started");
            TimeUnit.SECONDS.sleep(12);
            // quartz.addJob(String.valueOf(1), "JobB", "0/10 * * * * ?", JobB.class);
            quartz.addJob(String.valueOf(2), "JobA", "0/10 * * * * ?", JobA.class);
            quartz.addJob(String.valueOf(1), "JobB", "0/10 * * * * ?", JobB.class);
            quartz.showJobKeys("JobA");
            TimeUnit.SECONDS.sleep(12);
            quartz.deleteJobs("JobA");
            System.out.println("deleted JobA");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
