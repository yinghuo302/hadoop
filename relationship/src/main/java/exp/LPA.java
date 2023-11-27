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
import java.util.HashMap;
import java.util.StringTokenizer;

public class LPA {
    public static class LPAMapper extends Mapper<Text, Text, Text, Text> {
        private static final Text outKey = new Text();
        private static final Text outValue = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String splits[] = value.toString().split(";");
			StringTokenizer tokenizer = new StringTokenizer(splits[1],",;|");
			while (tokenizer.hasMoreTokens()){
				outKey.set(tokenizer.nextToken());
				outValue.set("@" + tokenizer.nextToken() + "," + splits[0]);
				context.write(outKey,outValue);
			}
            outValue.set("!"+splits[1]);
            context.write(key,outValue);
        }
    }

    public static class LPAReducer extends Reducer<Text, Text,Text,Text>{
        private static final Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String,Double> mp = new HashMap<>();
            Double maxWeight = 0.;  String edges = "";
			String maxLabel = Utils.hardLabel.getOrDefault(key.toString(),"");
			for(Text value:values){
				String str = value.toString();
				if(str.charAt(0)=='!'){
					edges = str.substring(1);
				}else if(!Utils.hardLabel.containsKey(key.toString())){
					int idx = str.indexOf(',');
					String label = str.substring(idx+1);
					Double weight = Double.parseDouble(str.substring(1,idx));
					Double newWeight = mp.getOrDefault(label,0.) + weight;
					if(newWeight > maxWeight) {
						maxWeight = newWeight;
						maxLabel = label;
					}
				}
			}
            outValue.set(maxLabel+";"+edges);
            context.write(key,outValue);
        }
    }
    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "LabelIter");
        job.setJarByClass(LPA.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(LPAMapper.class);
        job.setReducerClass(LPAReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}
