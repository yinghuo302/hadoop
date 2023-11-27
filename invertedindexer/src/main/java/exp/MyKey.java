package exp;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

	private Text firstKey;
	private DoubleWritable secondKey;

	// 无构造函数
	public MyKey() {
		this.firstKey = new Text();
		this.secondKey = new DoubleWritable();
	}

	public MyKey(Text firstKey, DoubleWritable secondKey) {
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}

	public MyKey(String firstKey, Double secondKey) {
		this.firstKey.set(firstKey);
		this.secondKey.set(secondKey);
	}

	public Text getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(Text firstKey) {
		this.firstKey = firstKey;
	}

	public DoubleWritable getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(DoubleWritable secondKey) {
		this.secondKey = secondKey;
	}

	public void set(String first,Double second){
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
		if (!secondKey.equals(key.secondKey)) {
			return secondKey.compareTo(key.secondKey);
		}else if (!firstKey.equals(key.firstKey)) {
			return firstKey.compareTo(key.firstKey);
		}  else {
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
		return firstKey.hashCode();
	}
}
