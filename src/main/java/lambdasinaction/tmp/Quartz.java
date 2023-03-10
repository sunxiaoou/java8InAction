package lambdasinaction.tmp;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Quartz {
    private static final Logger LOG = LoggerFactory.getLogger(Quartz.class);
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

    private String jsonToCornStr(JSONObject cronJson) {
        return (cronJson.containsKey("seconds") ? cronJson.getString("seconds") : "0") + " " +
                (cronJson.containsKey("minutes") ? cronJson.getString("minutes") : "*") + " " +
                (cronJson.containsKey("hours") ? cronJson.getString("hours") : "*") + " " +
                (cronJson.containsKey("dayOfMonth") ? cronJson.getString("dayOfMonth") : "*") + " " +
                (cronJson.containsKey("month") ? cronJson.getString("month") : "*") + " " +
                (cronJson.containsKey("dayOfWeek") ? cronJson.getString("dayOfWeek") : "?");
    }

    public void addJob(String group, JSONArray expressions, Class<? extends Job> jobClass) {
        int i = 0;
        for (Object expression: expressions) {
            addJob(String.valueOf(i), group, jsonToCornStr((JSONObject)expression), jobClass);
            i ++;
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

    public void pauseJobs(String group) {
        try {
            scheduler.pauseJobs(GroupMatcher.groupEquals(group));
            LOG.debug("group {}'s jobs paused", group);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void resumeJobs(String group) {
        try {
            scheduler.resumeJobs(GroupMatcher.groupEquals(group));
            LOG.debug("group {}'s jobs resumed", group);
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


    public static void main(String[] args) throws IOException {
        String path = "target/classes/lambdasinaction/tmp/schemaMap_policy.json";
        BufferedReader br = new BufferedReader(new FileReader(path));
        String jsonString = br.lines().collect(Collectors.joining("\n"));
        JSONArray cronExpr = JSON.parseObject(jsonString)
                .getJSONObject("policy")
                .getJSONArray("cronExpr");
        LOG.debug(cronExpr.toString());
        // System.out.println(cronExpr);

        try {
            Quartz quartz = new Quartz();
            quartz.start();
            String time = new SimpleDateFormat("yy-MM-dd HH-mm-ss").format(new Date());
            System.out.println("Quartz start at: " + time);
            TimeUnit.SECONDS.sleep(5);
            quartz.addJob("JobA", cronExpr, JobA.class);
            quartz.showJobKeys("JobA");
            TimeUnit.SECONDS.sleep(12);
            // quartz.addJob(String.valueOf(2), "JobA", "0/10 * * * * ?", JobA.class);
            // quartz.addJob(String.valueOf(0), "JobB", "0/10 * * * * ?", JobB.class);
            quartz.addJob(String.valueOf(0), "JobB", "6/7 * * * * ?", JobB.class);
            TimeUnit.SECONDS.sleep(12);
            // boolean b = quartz.deleteJobs("JobA");
            // System.out.println("deleted JobA " + b);
            quartz.pauseJobs("JobA");
            System.out.println("paused JobA");
            TimeUnit.SECONDS.sleep(20);
            quartz.resumeJobs("JobA");
            System.out.println("resumed JobA");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
