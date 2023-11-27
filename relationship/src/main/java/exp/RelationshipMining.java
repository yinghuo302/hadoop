package exp;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.LinkedList;

import static java.lang.Thread.sleep;

/**
 * Hello world!
 *
 */
public class RelationshipMining 
{
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: exp.RelationshipMining <name_list> <input> <output> <?NumOfIter>");
            System.exit(2);
        }
        conf.set("name-list",otherArgs[0]);
//		Preprocess.getJob(conf,otherArgs[1],otherArgs[2]+"/preprocess").waitForCompletion(true);
//		Cooccurrence.getJob(conf,otherArgs[2]+"/preprocess",otherArgs[2]+"/cooccurrence").waitForCompletion(true);
//		GraphBuilder.getJob(conf,otherArgs[2]+"/cooccurrence",otherArgs[2]+"/edges").waitForCompletion(true);
//		PageRank.getJob(conf,otherArgs[2]+"/edges",otherArgs[2]+"/PR/1").waitForCompletion(true);
//		PageRankView.getJob(conf,otherArgs[2]+"/PR/1",otherArgs[2]+"/PR/sort").waitForCompletion(true);
//		LPAInit.getJob(conf,otherArgs[2]+"/edges",otherArgs[2]+"/LPA/0").waitForCompletion(true);
//		LPA.getJob(conf,otherArgs[2]+"/LPA/0",otherArgs[2]+"/LPA/1").waitForCompletion(true);
//		LPAViewer.getJob(conf,otherArgs[2]+"/LPA/1",otherArgs[2]+"/LPA/viewer").waitForCompletion(true);


         JobControl jobFlow = new JobControl("Job");
		 ControlledJob job1 =new ControlledJob(Preprocess.getJob(conf,otherArgs[1],
                 otherArgs[2]+"/preprocess"),new ArrayList<>());
		 ControlledJob job2 = new ControlledJob(Cooccurrence.getJob(conf,otherArgs[2]+"/preprocess",
                 otherArgs[2]+"/cooccurrence"),Lists.newArrayList(job1));
		 ControlledJob job3 = new ControlledJob(GraphBuilder.getJob(conf,otherArgs[2]+"/cooccurrence",
                 otherArgs[2]+"/edges"),Lists.newArrayList(job2));
         jobFlow.addJob(job1); jobFlow.addJob(job2); jobFlow.addJob(job3);


         int cnt = 10;
         if(otherArgs.length==4)
             cnt = Integer.parseInt(otherArgs[3]);

         ControlledJob prevJob = job3;
         for(int i=0;i<cnt;i++){
             ControlledJob currJob = new ControlledJob(PageRank.getJob(conf,i!=0? otherArgs[2]+"/PR/" +i:
		 	 otherArgs[2]+"/edges",otherArgs[2]+"/PR/"+(i+1)), Lists.newArrayList(prevJob));
             jobFlow.addJob(currJob);
             prevJob = currJob;
         }
         jobFlow.addJob(new ControlledJob(PageRankView.getJob(conf, otherArgs[2]+"/PR/" + cnt,
                 otherArgs[2]+"/PR/sort"), Lists.newArrayList(prevJob)));

         prevJob = new ControlledJob(LPAInit.getJob(conf, otherArgs[2]+"/edges",
                 otherArgs[2]+"/LPA/0"),Lists.newArrayList(job3));
         jobFlow.addJob(prevJob);
         for(int i=0;i<cnt;i++){
             ControlledJob currJob = new ControlledJob(LPA.getJob(conf, otherArgs[2]+"/LPA/" +i,
                     otherArgs[2]+"/LPA/"+(i+1)),Lists.newArrayList(prevJob));
             jobFlow.addJob(currJob);
             prevJob = currJob;
         }
         jobFlow.addJob( new ControlledJob(LPAViewer.getJob(conf, otherArgs[2]+"/LPA/" + cnt,
                 otherArgs[2]+"/LPA/viewer"),Lists.newArrayList(prevJob)));

         Thread jobThread = new Thread(jobFlow);
         jobThread.start();

         while(true){
             if(jobFlow.allFinished()){
                 jobThread.stop(); return ;
             }
             if(!jobFlow.getFailedJobList().isEmpty() ){
                 jobThread.stop(); return ;
             }
             sleep(1000);
         }
    }
}
