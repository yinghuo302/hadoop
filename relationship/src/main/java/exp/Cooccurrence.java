package exp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Cooccurrence {
	public static class CoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text outKey = new Text();
        private static final LongWritable outValue = new LongWritable();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
			HashMap<String,Integer> cntMp = new HashMap<>();
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),",");
			while (tokenizer.hasMoreTokens()){
				String str = tokenizer.nextToken();
				cntMp.put(str,cntMp.getOrDefault(str,0) + 1);
			}
			for (Map.Entry<String, Integer> entry1 : cntMp.entrySet()){
				for (Map.Entry<String, Integer> entry2 : cntMp.entrySet()) {
					if (entry1.getKey().equals(entry2.getKey())) continue;
					outKey.set(entry1.getKey() + "#" + entry2.getKey());
					outValue.set(entry1.getValue() * entry2.getValue());
					context.write(outKey, outValue);
				}
			};
        }
    }

    public static class CoReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		private static final LongWritable outValue = new LongWritable(1);
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {
			long cnt = 0;
			for(LongWritable value:values){
				cnt += value.get();
			}
			outValue.set(cnt);
			context.write(key,outValue);
		}
	}

    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "Cooccurrence");
        job.setJarByClass(Cooccurrence.class);
        job.setMapperClass(CoMapper.class);
        job.setReducerClass(CoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}
