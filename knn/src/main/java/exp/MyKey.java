package exp;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

	private LongWritable firstKey;
	private DoubleWritable secondKey;

	public static class FirstGroupingComparator extends WritableComparator {
		public FirstGroupingComparator(){
			super(MyKey.class,true);
		}

		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			LongWritable l = ((MyKey)o1).getFirstKey();
			LongWritable r = ((MyKey)o2).getFirstKey();
			return l.compareTo(r);
		}

	}

	public static class FirstPartitioner extends Partitioner<MyKey,Text> {
		@Override
		public int getPartition(MyKey key, Text value,
								int numPartitions) {
			return Math.abs(key.getFirstKey().hashCode()  % numPartitions);
		}
	}
	// 无构造函数
	public MyKey() {
		this.firstKey = new LongWritable();
		this.secondKey = new DoubleWritable();
	}

	public MyKey(LongWritable firstKey, DoubleWritable secondKey) {
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}

	public MyKey(long firstKey, Double secondKey) {
		this.firstKey.set(firstKey);
		this.secondKey.set(secondKey);
	}

	public LongWritable getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(LongWritable firstKey) {
		this.firstKey = firstKey;
	}

	public DoubleWritable getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(DoubleWritable secondKey) {
		this.secondKey = secondKey;
	}

	public void set(long first,double second){
		this.firstKey.set(first);
		this.secondKey.set(second);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.firstKey.write(out);
		this.secondKey.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.firstKey.readFields(in);
		this.secondKey.readFields(in);
	}

	@Override
	public int compareTo(MyKey key) {
		if (!firstKey.equals(key.firstKey)) {
			return firstKey.compareTo(key.firstKey);
		} else if (!secondKey.equals(key.secondKey)) {
			return secondKey.compareTo(key.secondKey);
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MyKey)) {
			return false;
		}
		MyKey other = (MyKey) o;
		return other.firstKey.equals(firstKey) && other.secondKey.equals(secondKey);
	}

	@Override
	public int hashCode() {
		return firstKey.hashCode() * 31 + secondKey.hashCode();
	}
}
