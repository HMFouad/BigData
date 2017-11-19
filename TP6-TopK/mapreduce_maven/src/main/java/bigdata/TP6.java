package bigdata;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class TP6 {

	public static class TP6Mapper extends Mapper<Object, Text, NullWritable, Text> {

		public int k = 0;
		public SortedMap<Integer, String> mapperTopKCities = new TreeMap<Integer, String>();


		@Override
		public void setup(Context context) {
			k = Integer.parseInt(context.getConfiguration().get("K"));
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(",");
			if ((!data[4].isEmpty()) && (!data[4].matches("Population"))) {
				mapperTopKCities.put(Integer.parseInt(data[4]), data[4].concat(",").concat(data[2]));
				if (mapperTopKCities.size() > k) {
					mapperTopKCities.remove(mapperTopKCities.firstKey());
				}
			}
		}


		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (String cityName : mapperTopKCities.values()) {
				context.write(NullWritable.get(), new Text(cityName));
			}
		}
	}

	public static class TP6Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		public int k = 0;
		public SortedMap<Integer, String> reducerTopKCities = new TreeMap<Integer,String>();

		@Override
		public void setup(Context context) {
			k = context.getConfiguration().getInt("K", 10);
		}

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text cityData : values) {
				String[] data = cityData.toString().split(",");
				reducerTopKCities.put(Integer.parseInt(data[0]), cityData.toString());
				if (reducerTopKCities.size() > k) {
					reducerTopKCities.remove(reducerTopKCities.firstKey());
				}
			}
			for (String cityInfo : reducerTopKCities.values()) {
				context.write(key, new Text(cityInfo));
			}
		}
	}


	public static class TP6Combiner extends Reducer<NullWritable, Text, NullWritable, Text> {
		public int k = 0;
		public SortedMap<Integer, String> combinerTopKCities = new TreeMap<Integer,String>();

		@Override
		public void setup(Context context) {
			k = context.getConfiguration().getInt("K", 10);
		}

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text cityData : values) {
				String[] data = cityData.toString().split(",");
				combinerTopKCities.put(Integer.parseInt(data[0]), cityData.toString());
				if (combinerTopKCities.size() > k) {
					combinerTopKCities.remove(combinerTopKCities.firstKey());
				}
			}
			for (String cityInfo : combinerTopKCities.values()) {
				context.write(key, new Text(cityInfo));

			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("K", args[0]);
		Job job = Job.getInstance(conf, "TP6");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP6.class);
		job.setMapperClass(TP6Mapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(TP6Combiner.class);

		job.setReducerClass(TP6Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}