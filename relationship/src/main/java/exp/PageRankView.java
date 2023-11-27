package exp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PageRankView {
    public static Job getJob(Configuration conf,String inPath,String outPath) throws Exception {
        Job job = Job.getInstance(conf, "PageRankView");
        job.setJarByClass(PageRankView.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(ViewMapper.class);
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ViewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }

    public static class ViewMapper extends Mapper<Text, Text, MyKey, Text> {
        private String valueString = new String();
        private DoubleWritable pr = new DoubleWritable();
        private MyKey PRValue = new MyKey();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            valueString = value.toString();
            pr.set(Double.parseDouble(valueString.substring(0, valueString.indexOf(";"))));
            PRValue.set(pr);
            context.write(PRValue, key);
        }
    }
    public static class ViewReducer extends Reducer<MyKey, Text, Text, Text> {
        private Text Value = new Text();
        public void reduce(MyKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Value.set(key.get().toString());
            for (Text i : values) {
                context.write(i, Value);
            }
        }
    }

    public static class MyKey implements WritableComparable<MyKey> {
        private DoubleWritable key;
//        private Text

        public MyKey() {
            this.key = new DoubleWritable();
        }

        public MyKey(DoubleWritable key) {
            this.key = key;
        }

        public DoubleWritable get() {
            return this.key;
        }

        public void set(DoubleWritable key) {
            this.key = key;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.key.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.key.readFields(in);
        }

        @Override
        public int compareTo(MyKey key) {
            return key.get().compareTo(this.key);
        }

        @Override
        public int hashCode() {
            return this.key.hashCode();
        }
    }
}