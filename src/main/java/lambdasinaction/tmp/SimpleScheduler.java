package lambdasinaction.tmp;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by wanggenshen
 * Date: on 2018/7/7 16:31.
 * Description: XXX
 */
public class SimpleScheduler {
    public static void main(String[] args) throws SchedulerException, InterruptedException {
        // 1、创建调度器Scheduler
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        Scheduler scheduler = schedulerFactory.getScheduler();
        // 2、创建JobDetail实例，并与PrintWordsJob类绑定(Job执行内容)
        JobDetail jobDetail = JobBuilder.newJob(PrintWordsJob.class)
                .usingJobData("jobDetail", "this job is for test")
                .withIdentity("job", "group").build();
        // 3、构建Trigger实例,每隔1s执行一次
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("trigger", "triggerGroup")
                .usingJobData("trigger", "this trigger is for jobDetail")
                .startNow()//立即生效
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(1)//每隔1s执行一次
                        .repeatForever()).build();//一直执行

        //4、执行
        scheduler.scheduleJob(jobDetail, trigger);
        System.out.println("--------scheduler start ! ------------");
        scheduler.start();

        //睡眠
        TimeUnit.MINUTES.sleep(1);
        scheduler.shutdown();
        System.out.println("--------scheduler shutdown ! ------------");
    }
}
