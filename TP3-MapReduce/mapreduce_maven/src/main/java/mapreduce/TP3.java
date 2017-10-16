/*
TP3 Map Reduce
*** HOCINI Mohamed Fouad
	*** MAURICE Bastien
*/

package mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
  public static class TP3Mapper
       extends Mapper<Object, Text, Text, IntWritable>{
	  public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {

//*************************************************************************************************
		//Partie 1 "Les trois compteurs"		  

		  Counter validCities = context.getCounter("WCD", "nb_cities");

		  Counter citiesWithPop = context.getCounter("WCD", "nb_pop");
		  
		  Counter totalPop = context.getCounter("WCD", "total_pop");
		  		  
			//We split the line that the mapper is reading at every "," so that we can check
			  
			  String Data[]=value.toString().split(",");
			  
			  /*we increment the number of valid cities when we find  a valid city with
			  	Country(Data[0]!=''), city(Data[1]!='') and region(Data[2]!='')*/
			  if( (!Data[0].isEmpty()) && (!Data[1].isEmpty()) && (!Data[2].isEmpty()))
				  validCities.increment(1);

			 //whether or not the population of that city has been informed therefore the "if"			  
			  if (!Data[4].isEmpty())
			  {				 
				  /*The goal of this test is to make the mapper ignore the first line of the 
				   file because it contains only the name of columns*/
				  
				  if(!Data[4].matches("Population"))
				  {
					  
					  
					  /*For every city with an informed number of population => increment
					   the counter nb_pop by one, and the total_pop by the number of 
					   population of that city*/
					  
					  citiesWithPop.increment(1);
					  totalPop.increment(Integer.parseInt(Data[4]));
				  }
				  context.write(value, new IntWritable(1));
	}
//*************************************************************************************************
	//To test this part you must comment the code from lign 26 to 59.
		//Exercice 3: Partie 1 "Number of cities by country"
		  /*Text tmp=new Text(Data[0]);
		  if(!Data[0].matches("Country"))
		  if ( (!Data[0].isEmpty()) && (!Data[1].isEmpty()))
		  context.write(tmp, new IntWritable(1));*/
//*************************************************************************************************
	//To test this part you must comment the code bellow from lign 26 to 65.
		//Exercice 3: Partie 2 "Population by country"
			  /*if(!Data[4].matches("Population"))
			  if ( (!Data[0].isEmpty()) && (!Data[4].isEmpty()))
				  
			  context.write(new Text(Data[0]), new IntWritable(Integer.parseInt(Data[4])));*/
//*************************************************************************************************		  
		    }
  }
  public static class TP3Reducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

		  context.write(key, null);
		  
//*************************************************************************************************		
	// To test the part 1,2 from the 3rd exercice you must comment the ligne 83 and let the code bellow  
		  //Exercice 3: Partie 1+2 "Number of cities by country" + "Population by country"
		  /*int count = 0;
		  for(IntWritable val:values)
		  count +=val.get();		  
		  context.write(key, new IntWritable(count));*/
		  
    	 
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}