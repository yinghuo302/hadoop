package exp;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KNN {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: exp.KNN <K> <traindata> <testdata> <out>");
			System.exit(2);
		}
		conf.set("K",otherArgs[0]);
		Job job = Job.getInstance(conf, "KNN");
		DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
		job.setJarByClass(KNN.class);
		job.setMapperClass(KNNMapper.class);
		job.setReducerClass(KNNReducer.class);
		job.setPartitionerClass(MyKey.FirstPartitioner.class);
		job.setGroupingComparatorClass(MyKey.FirstGroupingComparator.class);
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class KNNMapper extends Mapper<LongWritable, Text, MyKey, Text> {
		private final Text str = new Text();
		private final MyKey mpKey = new MyKey();

		public static double getDistance(double[] trainPoint, double[] testPoint){
			double dis = 0;
			for(int i=0;i<trainPoint.length;i++)
				dis += (trainPoint[i] - testPoint[i]) * (trainPoint[i] - testPoint[i]) ;
			return dis;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");
			int n = splits.length-1;
			double[] trainPoint = new double[n];
			for (int i = 0; i < n; i++)
				trainPoint[i] = Double.parseDouble(splits[i]);
			String className = splits[n];
			Configuration conf = context.getConfiguration();
			Path[] path = DistributedCache.getLocalCacheFiles(conf);
			FileSystem fsopen = FileSystem.getLocal(conf);
			FSDataInputStream in = fsopen.open(path[0]);
			Scanner scanner = new Scanner(in);
			long lnNumber = 0;
			while (scanner.hasNext()) {
				splits = scanner.nextLine().split(",");
				double[] testPoint = new double[splits.length];
				for (int i = 0; i < splits.length; i++)
					testPoint[i] = Double.parseDouble(splits[i]);
				double distance = getDistance(trainPoint, testPoint);
				str.set(distance + "-" + className);
				mpKey.set(lnNumber++,distance);
				context.write(mpKey, str);
			}
			scanner.close();
		}
	}

	public static class KNNReducer extends Reducer<MyKey, Text, LongWritable, Text> {
		private Text result = new Text();
		private int k = 1;
		private static double getWeight(double distance){
			return 16 * Math.exp(-1 * distance);
		}
		public void reduce(MyKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Map<String, Double> map = new HashMap<>();
			int num = 0;Double max_weight = 0.;String max_class = "";
			for(Text value:values){
				num++;
				String[] ret = value.toString().split("-");
				Double newValue = map.getOrDefault(ret[1], 0.) 
					+ getWeight(Double.parseDouble(ret[0]));
				map.put(ret[1],newValue);
				if(newValue>max_weight) {
					max_weight = newValue;
					max_class = ret[1];
				}
				if(num==this.k) break;
			}
			result.set(max_class);
			context.write(key.getFirstKey(), result);
		}
		@Override
        protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("K",1);

		}
	}



}