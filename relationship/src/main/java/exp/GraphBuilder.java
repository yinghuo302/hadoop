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
import java.util.ArrayList;

public class GraphBuilder {
	public static class GBMapper extends Mapper<Text, Text, Text, Text> {
        private static final Text outKey = new Text();
        private static final Text outValue = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splits = key.toString().split("#");
            outKey.set(splits[0]);
            outValue.set(splits[1]+","+value.toString());
            context.write(outKey,outValue);
        }
    }

    public static class GBReducer extends Reducer<Text, Text,Text,Text> {
        private static final Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            builder.append("1;");
            ArrayList<MyPair> list = new ArrayList<>();
            Long all = 0l;
            for(Text value :values){
                String[] splits = value.toString().split(",");
                Long tem = Long.parseLong(splits[1]);
                all += tem;
                list.add(new MyPair(splits[0],tem));
            }
            for(MyPair pair: list){
                builder.append(String.format("%s,%.5f|",pair.first,Double.valueOf(pair.second) / all));
            }
            outValue.set(builder.toString());
            context.write(key,outValue);
        }
    }

    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "GraphBuilder");
        job.setJarByClass(GraphBuilder.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(GBMapper.class);
        job.setReducerClass(GBReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
    private static class MyPair{
        public String first;
        public Long second;
        public MyPair(String first,Long second){
            this.first = first;
            this.second = second;
        }
    }
    
}


