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
import java.util.StringTokenizer;


public class PageRank {
    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "PageRank");
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(IterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }

    public static class IterMapper extends Mapper<Text, Text, Text, Text> {
        private String valueString = new String();
        private Text linkList = new Text();
        private Text name = new Text();
        private Double currValue = 0.;
        private Text prValue = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            valueString = value.toString();
            linkList.set("1" + valueString.substring(1 + valueString.indexOf(";")));
            context.write(key, linkList);
            StringTokenizer words = new StringTokenizer(valueString,",;|");
            currValue = Double.parseDouble(words.nextToken());
            while (words.hasMoreTokens()) {
                name.set(words.nextToken());
                prValue.set("s" + (currValue * Double.parseDouble(words.nextToken())));
                context.write(name, prValue);
            }
        }
    }

    public static class IterReducer extends Reducer<Text, Text, Text, Text> {
        private String linkList = "";
        private String word = "";
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Double sum = 0.;
            for (Text i : values) {
                word = i.toString();
                if (word.charAt(0) == '1') {
                    linkList = word.substring(1);
                }
                else {
                    sum += Double.parseDouble(word.substring(1));
                }
            }
            result.set(String.valueOf(sum) + ";" + linkList);
            context.write(key, result);
        }
    }
}