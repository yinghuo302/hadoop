package exp;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Preprocess {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private static final Text outValue = new Text();
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			outValue.set(String.join(",",Utils.tokenize(value.toString())));
			context.write(outValue, NullWritable.get());
		}


		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			Path[] path = DistributedCache.getLocalCacheFiles(conf);
			FileSystem fs = FileSystem.getLocal(conf);
			Scanner scanner = new Scanner(fs.open(path[0]));
			while (scanner.hasNext()){
				Utils.insertName(scanner.nextLine());
			}
		}

	}

	public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
		Job job = Job.getInstance(conf, "proprocess");
		DistributedCache.addCacheFile(new URI(conf.get("name-list")), job.getConfiguration());
		job.setJarByClass(Preprocess.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		return job;
	}
}