package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyWritable implements Writable {
	private Integer counter;
	private Integer min;
	private Integer max;
	private Integer avg;
	
	
	//Default consructor
	public MyWritable(Integer counter, Integer avg, Integer max, Integer min)
	{
		
		this.counter = counter;
		this.avg = avg;
		this.max = max;
		this.min = min;
	}
	
	public void readFields(DataInput in) throws IOException {
		

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(counter);
		out.writeInt(avg);
		out.writeInt(min);
		out.writeInt(max);
		
	}
	
	public Text writeMywritable() {
		return(new Text(Integer.toString(this.counter)+"\t"+Integer.toString(this.avg)+"\t"+Integer.toString(this.max)+"\t"+Integer.toString(this.min)));
		
	}
	
}
