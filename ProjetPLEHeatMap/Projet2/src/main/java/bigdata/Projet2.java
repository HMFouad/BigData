package bigdata;

import java.io.IOException;
import java.io.Serializable;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.annotation.meta.beanGetter;

public class Projet2 {

	private static final byte[] FAMILY1 = Bytes.toBytes("Dem3.0");
	private static final byte[] FAMILY2 = Bytes.toBytes("Dem3.1");
	private static final byte[] FAMILY3 = Bytes.toBytes("Dem3.2");
	
	private static final byte[] TABLE1_NAME = Bytes.toBytes("HMDem3.0");
	private static final byte[] TABLE2_NAME = Bytes.toBytes("HMDem3.1");
	private static final byte[] TABLE3_NAME = Bytes.toBytes("HMDem3.2");

	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}

	public static void createTable(Connection connect, byte[] table, byte[] family) {
		try {
			final Admin admin = connect.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table));
			HColumnDescriptor famLoc = new HColumnDescriptor(family);
			// famLoc.set...
			tableDescriptor.addFamily(famLoc);
			createOrOverwrite(admin, tableDescriptor);
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void main(String[] args) throws Exception {

		Connection connection = ConnectionFactory.createConnection();
		createTable(connection, TABLE1_NAME, FAMILY1);
		createTable(connection, TABLE2_NAME, FAMILY2);
		createTable(connection, TABLE2_NAME, FAMILY2);
		
		Table table1 = connection.getTable(TableName.valueOf(TABLE1_NAME));
		Table table2 = connection.getTable(TableName.valueOf(TABLE2_NAME));
		Table table3 = connection.getTable(TableName.valueOf(TABLE3_NAME));

		SparkConf conff = new SparkConf().setAppName("Write Rdd to hbase");
		JavaSparkContext context = new JavaSparkContext(conff);

		JavaRDD<String> records = context.textFile(args[0]);
		// ne pas recalculer les RDD
		records.cache();

		JavaRDD<Dem3Cordonnees> data = records.map(x -> {
			String[] tokens = x.split(",");
			Pattern pattern = Pattern.compile("[0-9]");

			double latitude = -100;
			if (tokens[0] != "" && pattern.matcher(tokens[0]).find()) {
				latitude = Double.parseDouble(tokens[0]);
			}
			;

			double longitude = -200;
			if (tokens[1] != "" && pattern.matcher(tokens[1]).find()) {
				longitude = Double.parseDouble(tokens[1]);
			}
			;

			double hauteur = -1;
			if (tokens[2] != "" && pattern.matcher(tokens[2]).find()) {
				longitude = Double.parseDouble(tokens[2]);
			}
			;

			Dem3Cordonnees dem3 = new Dem3Cordonnees(latitude, longitude, hauteur);

			return dem3;
		});

		JavaRDD<Dem3Cordonnees> rddFiltred = data.filter((x) -> x.hauteur != -1 && x.latitude < 90 && x.latitude > -90
				&& x.longitude < -180  && x.longitude > -180);
		//List<Dem3Cordonnees> mydata = rddFiltred.collect();
		
		Function2<Double, Double, Double> sumFunc = (x, y) -> (x+y);
		
		Function2<Integer, Integer, Integer> totale = (x, y) -> (x+y);
		
		
		double latMin = -90;
		double latMax = 90;
		double longMin = -180;
		double longMax = 180;

		//Niveau de zoom 1 
		double pasLat= 1.40625;
		double pasLong= 2.8125;
				
		for (double i= latMin; i<latMax; i+=pasLat) { 
			
			double indicei = i;
			double paslatitude1 = pasLat;
			
			for (double j= longMin; j<longMax; j+=pasLong) {
				double indicej = j;
				double paslongitude1 = pasLong;
				
				JavaRDD<Dem3Cordonnees> rddIntervalle = rddFiltred.filter((x) -> x.latitude < (indicei+paslatitude1) 
						&& x.latitude > indicei && x.longitude < indicej && x.longitude > indicej+paslongitude1);

				JavaPairRDD<PointGPS, Double> avgposition = rddIntervalle
						.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Double>() {
							@Override
							public Tuple2<PointGPS, Double> call(Dem3Cordonnees intervale) throws Exception {	
								return new Tuple2<PointGPS, Double>(new PointGPS((indicei+paslatitude1)/2,(indicej+paslongitude1)/2), intervale.hauteur);
							}
						});
				
								
				JavaPairRDD<PointGPS, Integer> nb1 = rddIntervalle
						.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Integer>() {
							@Override
							public Tuple2<PointGPS, Integer> call(Dem3Cordonnees intervale) throws Exception {	
								return new Tuple2<PointGPS, Integer>(new PointGPS((indicei+paslatitude1)/2,(indicej+paslongitude1)/2), 1);
							}
						});
				
				//Sum hauteur de chaque intervale
				JavaPairRDD<PointGPS, Double> sumIntervalle = avgposition.reduceByKey(sumFunc);
				//Nombre d'apparition d'un pays
				//JavaPairRDD<PointGPS, Integer> nbb = nb.reduceByKey(totale);
				
				List<Tuple2<PointGPS,Double>> infoIntervalle = sumIntervalle.collect();
				
				for (int k = 0; k < infoIntervalle.size() ; k++) {
					Put put = new Put(Bytes.toBytes("ROW" + k));
					
					put.addColumn(FAMILY1, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.latitude)));
					put.addColumn(FAMILY1, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.longitude)));
					put.addColumn(FAMILY1, Bytes.toBytes("hauteur"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._2)));
					
					table1.put(put);
				}
						
			}
			}
		
		//Niveau de zoom 2
		pasLat= 2.8125;
		pasLong= 5.625;
				
		for (double i= latMin; i<latMax; i+=pasLat) { 
			double indicei = i;
			double paslatitude2 = pasLat;
			
			for (double j= longMin; j<longMax; j+=pasLong) {
				double indicej = j;
				double paslongitude2 = pasLong;
				
				JavaRDD<Dem3Cordonnees> rddIntervalle = rddFiltred.filter((x) -> x.latitude < (indicei+paslatitude2) 
						&& x.latitude > indicei && x.longitude < indicej && x.longitude > indicej+paslongitude2);

				JavaPairRDD<PointGPS, Double> avgposition = rddIntervalle
						.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Double>() {
							@Override
							public Tuple2<PointGPS, Double> call(Dem3Cordonnees intervale) throws Exception {	
								return new Tuple2<PointGPS, Double>(new PointGPS((indicei+paslatitude2)/2,(indicej+paslongitude2)/2), intervale.hauteur);
							}
						});
				
								
				JavaPairRDD<PointGPS, Integer> nb2 = rddIntervalle
						.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Integer>() {
							@Override
							public Tuple2<PointGPS, Integer> call(Dem3Cordonnees intervale) throws Exception {	
								return new Tuple2<PointGPS, Integer>(new PointGPS((indicei+paslatitude2)/2,(indicej+paslongitude2)/2), 1);
							}
						});
				
				//Sum hauteur de chaque intervale
				JavaPairRDD<PointGPS, Double> sumIntervalle = avgposition.reduceByKey(sumFunc);
				//Nombre d'apparition d'un pays
				//JavaPairRDD<PointGPS, Integer> nbb = nb.reduceByKey(totale);
				
				List<Tuple2<PointGPS,Double>> infoIntervalle = sumIntervalle.collect();
				
				for (int k = 0; k < infoIntervalle.size() ; k++) {
					Put put = new Put(Bytes.toBytes("ROW" + k));
					
					put.addColumn(FAMILY2, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.latitude)));
					put.addColumn(FAMILY2, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.longitude)));
					put.addColumn(FAMILY2, Bytes.toBytes("hauteur"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._2)));
					
					table2.put(put);
				}
						
			}
			}
		
		//Niveau de zoom 3
				pasLat= 5.625;
				pasLong= 11.25;
						
				for (double i= latMin; i<latMax; i+=pasLat) { 
					double indicei = i;
					double paslatitude3 = pasLat;
					for (double j= longMin; j<longMax; j+=pasLong) {
						double indicej = j;
						double pasLongitude3 = pasLong;
						
						JavaRDD<Dem3Cordonnees> rddIntervalle = rddFiltred.filter((x) -> x.latitude < (indicei+paslatitude3) 
								&& x.latitude > indicei && x.longitude < indicej && x.longitude > indicej+pasLongitude3);

						JavaPairRDD<PointGPS, Double> avgposition = rddIntervalle
								.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Double>() {
									@Override
									public Tuple2<PointGPS, Double> call(Dem3Cordonnees intervale) throws Exception {	
										return new Tuple2<PointGPS, Double>(new PointGPS((indicei+paslatitude3)/2,(indicej+pasLongitude3)/2), intervale.hauteur);
									}
								});
						
										
						JavaPairRDD<PointGPS, Integer> nb3 = rddIntervalle
								.mapToPair(new PairFunction<Dem3Cordonnees, PointGPS, Integer>() {
									@Override
									public Tuple2<PointGPS, Integer> call(Dem3Cordonnees intervale) throws Exception {	
										return new Tuple2<PointGPS, Integer>(new PointGPS((indicei+paslatitude3)/2,(indicej+pasLongitude3)/2), 1);
									}
								});
						
						//Sum hauteur de chaque intervale
						JavaPairRDD<PointGPS, Double> sumIntervalle = avgposition.reduceByKey(sumFunc);
						//Nombre d'apparition d'un pays
						//JavaPairRDD<PointGPS, Integer> nbb = nb.reduceByKey(totale);
						
						List<Tuple2<PointGPS,Double>> infoIntervalle = sumIntervalle.collect();
						
						for (int k = 0; k < infoIntervalle.size() ; k++) {
							Put put = new Put(Bytes.toBytes("ROW" + k));
							
							put.addColumn(FAMILY3, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.latitude)));
							put.addColumn(FAMILY3, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._1.longitude)));
							put.addColumn(FAMILY3, Bytes.toBytes("hauteur"), Bytes.toBytes(Double.toString(infoIntervalle.get(k)._2)));
							
							table3.put(put);
						}
								
					}
					}

	}
}
