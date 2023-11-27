package exp;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex
{
	public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2){ 
			System.err.println("Usage: InvertedIndex <in> <out1> <out2> <out3>");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		int fileCount = fs.listStatus(new Path(otherArgs[0])).length;
		conf.set("DocCount",String.valueOf(fileCount));
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
		if(otherArgs.length>=3){
			Job job1 = Job.getInstance(conf, "Sort");
			job1.setJarByClass(InvertedIndex.class);
			job1.setMapperClass(SortMapper.class);
			job1.setMapOutputKeyClass(MyKey.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setReducerClass(SortReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
			job1.waitForCompletion(true);
		}
		if(otherArgs.length>=4){
			Job job2 = Job.getInstance(conf, "TF-IDF");
			job2.setJarByClass(InvertedIndex.class);
			job2.setMapperClass(TFIDFMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setReducerClass(TFIDFReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
			job2.waitForCompletion(true);
		}
    }
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

		private final Text word = new Text();
		private final Text docName = new Text();

	
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String doc = ((FileSplit) context.getInputSplit()).getPath().getName();
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set('[' + token+ ']');
				docName.set(doc);
				context.write(word, docName);
			}
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

		private final Text docList = new Text();
	
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int total = 0;
			Map<String, Integer> map = new HashMap<>();
			for (Text value : values) {
				String doc = value.toString();
				map.put(doc,map.getOrDefault(doc, 0)+1);					
				++total;
			}
			StringBuilder builder = new StringBuilder();
			builder.append(String.format("%.2f,", (double)total /map.size() ) );
			map.forEach((_key,value) ->{
				builder.append(_key+':'+value+';');
			});
			builder.deleteCharAt(builder.length()-1);
			docList.set(builder.toString());
			context.write(key, docList);
		}
	}


	public static class SortMapper extends Mapper<LongWritable, Text, MyKey, Text> {
		private final Text docList = new Text();
		private final MyKey myKey = new MyKey();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t,");
			String word = tokenizer.nextToken();
			String avgNumStr = tokenizer.nextToken();
			Double avgNum = Double.parseDouble(avgNumStr);
			docList.set(avgNumStr +','+ tokenizer.nextToken());
			myKey.set(word,avgNum);
			context.write(myKey, docList);
		}
	}

	public static class SortReducer extends Reducer<MyKey, Text, Text, Text> {

		public void reduce(MyKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for(Text value : values) {
				context.write(key.getFirstKey(),value);
			}
		}
	}

	public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final Text doc = new Text();
		private final Text wordCnt = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString(),"\t,:;");
			int docCount = tokenizer.countTokens() / 2 - 1;
			String word = tokenizer.nextToken();
			tokenizer.nextToken(); // 平均出现次数,不需要
			while (tokenizer.hasMoreTokens()) {
				doc.set(tokenizer.nextToken());
				wordCnt.set(word +':'+tokenizer.nextToken()+
						':'+String.valueOf(docCount));
				context.write(doc,wordCnt);
			}
		}
	}

	public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		private final Text word_doc = new Text();
		private final DoubleWritable tfidf = new DoubleWritable();

		private int docCount = 1;

		@Override
		protected void setup(Reducer<Text, Text, Text, DoubleWritable>.Context context) {
			docCount = context.getConfiguration().getInt("DocCount",1);
		}
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int all_word = 0; String tem;
			List<String> all_items = new ArrayList<>();
			for(Text value : values) {
				tem = value.toString();
				all_items.add(tem);
				all_word += Integer.parseInt( tem.split(":")[1]);
			}
			for(String item: all_items){
				String[] splits = item.toString().split(":");
				word_doc.set(key.toString() + ',' + splits[0]);
				Double idf = Math.log((double) docCount / (Integer.parseInt(splits[2])+1) );
				tfidf.set(Integer.parseInt(splits[1]) * idf /all_word);
				context.write(word_doc,tfidf);
			}
		}
	}

}


