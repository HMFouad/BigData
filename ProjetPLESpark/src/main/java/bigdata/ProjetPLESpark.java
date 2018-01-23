package bigdata;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ProjetPLESpark {

	public static void main(String[] args) {

		String inputFile = args[0];
		String outputFile = args[1];
		
		SparkConf conf = new SparkConf().setAppName("ProjetPLE");
		
		JavaSparkContext context = new JavaSparkContext(conf);

	    // Load our input data.
		JavaRDD<String> lines = context.textFile(inputFile);
				
		JavaRDD<Object> words = lines.map( x -> {
			String [] tokens = x.split(",");
			
			return tokens;
		});
		
		//JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(",")).iterator()); 
		
		/*	
        // flatMap each line to words in the line
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(",")).iterator();
			}
		}); 
		*/
		
		
		System.out.println("*** Yes !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        // collect RDD for printing
        for(Object word:words.collect()){
        	words.saveAsTextFile(outputFile);
            //System.out.println(word);
        }
		
		
	}

}

