package exp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class LPAViewer {
    public static class LPAViewerMapper extends Mapper<Text, Text, Text, Text> {
        private static final Text outKey = new Text();
        private static final Text outValue = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            outKey.set(value.toString().split(";")[0]);
            context.write(outKey,key);
        }
    }

    public static class LPAViewerReducer extends Reducer<Text, Text,Text,Text> {
        private static final Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for(Text value :values){
                builder.append(value.toString()).append('|');
            }
            outValue.set(builder.toString());
            context.write(key,outValue);
        }
    }
    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "LabelViewer");
        job.setJarByClass(LPAViewer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(LPAViewerMapper.class);
        job.setReducerClass(LPAViewerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}
