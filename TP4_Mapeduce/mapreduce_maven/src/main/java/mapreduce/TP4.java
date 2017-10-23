package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class TP4  extends Configured implements Tool {
  public static class TP4Mapper
       extends Mapper<Object, Text, Text, IntWritable>{
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
		// Spliting the line that the mapper is reading at every ","
		  String Data[]=value.toString().split(",");
	    	
		// Cheking that the population of that city has been informed and deleting the first line
	    	if(!Data[4].isEmpty())
	    		if(!Data[4].matches("Population"))
	    		{
	    			// By using Math.Log10() and Math.pow() we will get the categories 10,100,1000 ...
	    			Double popLog = Math.log10(Double.parseDouble(Data[4]));
	    			
	    			Integer logRound = (int) Math.floor(popLog);

	    			Integer pow = (int) Math.pow(10, logRound);
	    			
	    			 // Context.getConfiguration() to get "FragmentRate"
	    			int nbFragment = Integer.parseInt((context.getConfiguration().get("nbFragment")));
	    			
	    			//Interval of population
	    			double interval = (pow*10)/nbFragment;
	    			
	    			int Temp = (int) (Integer.parseInt(Data[4]) / interval);
	    			
	    			String st = String.valueOf((int) (Temp * interval));
	    			
	    			context.write(new Text(st), new IntWritable(Integer.parseInt(Data[4])));
	    			
	    		}	
	    		   	    	
	  }
  }
  
  public static class TP4Reducer
       extends Reducer<Text,IntWritable,Text,Text> {
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        	
    	int counter = 0;
		int min = 999999999;
		int max = 1;
		int sum=0;
		int avg=0;
		
		String result0= "";
		
		//For the firt line to have the same as the exemple
		context.write(null, new Text("count"+"\t"+"avg"+"\t"+"max"+"\t"+"min"));
		
    	for(IntWritable val:values) {
    		result0= "";
		     		
    	  counter ++;
    	  sum += val.get();
    	  if (min > val.get()) min = val.get();
    	  if (max < val.get()) max = val.get();
    	    	
  
    }
    	
    	avg = (int) (sum/counter);
    	
    	MyWritable myWritable = new MyWritable(counter, avg, max, min);
    	
    	
  	  //Create new String with all that we need
		result0 = Integer.toString(counter)+"\t"+Integer.toString(avg)+"\t"+Integer.toString(max)+"\t"+Integer.toString(min);
			     	
    	//context.write(key, new Text(result0));
		
		context.write(key, myWritable.writeMywritable());
    	
  }
  }

public int run(String[] arg0) throws Exception {
	// TODO Auto-generated method stub
	return 0;
}
 
  
  /*public static void main(String[] args) throws Exception {
	  	//System.exit(ToolRunner.run(new FilterCities(), args));
	  
	  	
		System.exit(ToolRunner.run(new TP4(), args));
  }*/
}
