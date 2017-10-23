package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapreduce.FilterCities.CitiesWithPopMapper;
import mapreduce.FilterCities.MaxReduce;
import mapreduce.TP4.TP4Mapper;
import mapreduce.TP4.TP4Reducer;

public class Main{
	static int nbFragment;
	static Path path;

	public static void main(String[] args) throws Exception, IOException, ClassNotFoundException, InterruptedException {{	
		try {
			
			nbFragment=Integer.valueOf(args[3]);
			
			}catch(Exception e){
				throw new IllegalArgumentException(" Passez un entier en parametre");
				//System.out.println(e.getMessage());
				
			}

			//public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf0 = new Configuration();
			conf0.set("nbFragment", args[0]);
			Job job0 = Job.getInstance(conf0, "FilterCities");
			job0.setNumReduceTasks(1);
			job0.setJarByClass(FilterCities.class);
			job0.setOutputKeyClass(Text.class);
			job0.setOutputValueClass(Text.class);
			job0.setOutputFormatClass(TextOutputFormat.class);
			job0.setInputFormatClass(TextInputFormat.class);
			
			job0.setMapperClass(CitiesWithPopMapper.class);
			job0.setReducerClass(MaxReduce.class);
			
			try {
				FileInputFormat.addInputPath(job0, new Path(args[1]));
				path=new Path(args[1]);
				FileOutputFormat.setOutputPath(job0, path);
			} 
			catch (Exception e) {
				System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
				return;
			}
			//return job.waitForCompletion(true) ? 0 : 1;

		
		  	  
			  	Configuration conf = new Configuration();	
			  	conf.setInt("para", param);
			  	Job job1 = Job.getInstance(conf, "TP4");
			    job1.setNumReduceTasks(1);
			    job1.setJarByClass(TP4.class);
			    job1.setMapperClass(TP4Mapper.class);
			    job1.setMapOutputKeyClass(Text.class);
			    job1.setMapOutputValueClass(IntWritable.class);
			    job1.setReducerClass(TP4Reducer.class);
				job1.setMapperClass(TP4Mapper.class);
				job1.setReducerClass(TP4Reducer.class);
			    job1.setOutputKeyClass(Text.class);
			    job1.setOutputValueClass(IntWritable.class);
			    job1.setOutputFormatClass(TextOutputFormat.class);
			    job1.setInputFormatClass(TextInputFormat.class);
			    
				try {
					FileInputFormat.addInputPath(job1, path);
					FileOutputFormat.setOutputPath(job1, new Path(args[2]));
				} 
				catch (Exception e) {
					System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
					return;
				}

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
	}
}