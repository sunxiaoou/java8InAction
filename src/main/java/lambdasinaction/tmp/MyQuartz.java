package lambdasinaction.tmp;

import com.alibaba.fastjson.JSONObject;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class MyQuartz {
    public static JSONObject cronStrToJson(String cronStr) {
        JSONObject jsonObject = new JSONObject();
        List<String> cronParts = Arrays.asList(cronStr.split("\\s+"));
        if (cronParts.size() > 5) {
            jsonObject.put("seconds", cronParts.get(0));
            cronParts =  cronParts.subList(1, 6);
        }
        jsonObject.put("minutes", cronParts.get(0));
        jsonObject.put("hours", cronParts.get(1));
        jsonObject.put("dayOfMonth", cronParts.get(2));
        jsonObject.put("month", cronParts.get(3));
        jsonObject.put("dayOfWeek", cronParts.get(4));
        return jsonObject;
    }

    public static String jsonToCornStr(JSONObject cronJson) {
        return  (cronJson.containsKey("seconds") ? cronJson.getString("seconds") : "0") + " " +
                (cronJson.containsKey("minutes") ? cronJson.getString("minutes") : "*") + " " +
                (cronJson.containsKey("hours") ? cronJson.getString("hours") : "*") + " " +
                (cronJson.containsKey("dayOfMonth") ? cronJson.getString("dayOfMonth") : "*") + " " +
                (cronJson.containsKey("month") ? cronJson.getString("month") : "*") + " " +
                (cronJson.containsKey("dayOfWeek") ? cronJson.getString("dayOfWeek") : "?");
    }

    public static void myQuartz(String cronStr) throws SchedulerException, ParseException {
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
                .withSchedule(cronSchedule(cronStr))
                // .withSchedule(cronSchedule("0 50 10 * * ?"))
                .build();

        // Schedule job with the trigger
        scheduler.scheduleJob(job, trigger);

        // Start the scheduler
        scheduler.start();
    }

    public static void main(String[] args) throws ParseException, SchedulerException {
        String cronStr = "0 31 17 * * ?";
        // String cronStr = "0 0/1 * * * ?";
        // String cronStr = "0/10 * * * * ?";
        JSONObject cronJson = MyQuartz.cronStrToJson(cronStr);
        System.out.println(cronJson.toString());
        System.out.println(MyQuartz.jsonToCornStr(cronJson));
        MyQuartz.myQuartz(cronStr);
    }
}
