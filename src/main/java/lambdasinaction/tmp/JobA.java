package lambdasinaction.tmp;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class JobA implements Job {
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String printTime = new SimpleDateFormat("yy-MM-dd HH-mm-ss").format(new Date());
        // System.out.println("jobA start at:" + printTime + ", prints: Hello Job-" + new Random().nextInt(100));
        System.out.println(jobExecutionContext.getJobDetail().getKey() +  " at: " + printTime + " " +
                new Random().nextInt(100));
    }
}
