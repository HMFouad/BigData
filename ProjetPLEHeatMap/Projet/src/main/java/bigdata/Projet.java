package bigdata;

import java.io.IOException;
import java.io.Serializable;
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

import com.sun.tools.classfile.Annotation.element_value;

import scala.Function1;
import scala.Function3;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.api.java.function.*;

public class Projet {

	private static final byte[] FAMILY = Bytes.toBytes("WorldCitiesAll");
	private static final byte[] FAMILY_Pays = Bytes.toBytes("WorldCitiesByCountry");
	private static final byte[] FAMILY_Region = Bytes.toBytes("WorldCitiesByRegion");
	private static final byte[] FAMILY_City = Bytes.toBytes("WorldCitiesByCity");
	
	private static final byte[] TABLE_NAME = Bytes.toBytes("worldCitiesAll");
	private static final byte[] TABLE1_NAME = Bytes.toBytes("worldCitiesCountry");
	private static final byte[] TABLE2_NAME = Bytes.toBytes("worldCitiesRegion");
	private static final byte[] TABLE3_NAME = Bytes.toBytes("worldCitiesCity");

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
		createTable(connection, TABLE_NAME, FAMILY);
		createTable(connection, TABLE1_NAME, FAMILY_Pays);
		createTable(connection, TABLE2_NAME, FAMILY_Region);
		createTable(connection, TABLE3_NAME, FAMILY_City);
		
		Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		Table tableCountry = connection.getTable(TableName.valueOf(TABLE1_NAME));
		Table tableRegion = connection.getTable(TableName.valueOf(TABLE2_NAME));
		Table tableCity = connection.getTable(TableName.valueOf(TABLE3_NAME));

		SparkConf conff = new SparkConf().setAppName("Write Rdd to hbase");
		JavaSparkContext context = new JavaSparkContext(conff);

		JavaRDD<String> records = context.textFile(args[0]);
		//JavaRDD<WorldCitiesCordonnees> distData = context.parallelize(records);
		
		// ne pas recalculer les RDD
		records.cache();
		
		JavaRDD<WorldCitiesCordonnees> data = records.map(x -> {
			String[] tokens = x.split(",");
			Pattern pattern = Pattern.compile("[0-9]");

			String pays = "";
			if (tokens[0] != "") {
				pays = tokens[0];
			}
			;

			String ville = "";
			if (tokens[1] != "") {
				ville = tokens[1];
			}
			;

			String region = "";
			if (tokens[3] != "") {
				region = tokens[3];
			}
			;

			double population = -1;
			if (tokens[4] != "" && pattern.matcher(tokens[4]).find()) {
				population = Double.parseDouble(tokens[4]);
			}
			;

			double latitude = -100;
			if (tokens[5] != "" && pattern.matcher(tokens[5]).find()) {
				latitude = Double.parseDouble(tokens[5]);
			}
			;

			double longitude = -200;
			if (tokens[6] != "" && pattern.matcher(tokens[6]).find()) {
				longitude = Double.parseDouble(tokens[6]);
			}
			;

			WorldCitiesCordonnees worldcities = new WorldCitiesCordonnees(pays, ville, region, population, latitude,
					longitude);

			return worldcities;
		});

		
		//Filtre pour garder que les villes valides avec tout les champs
		JavaRDD<WorldCitiesCordonnees> rddFiltred = data.filter((x) -> x.population != -1 && x.pays != "" && x.region != "" && x.ville != "" && x.latitude < 91
				&& x.latitude > -91 && x.longitude < 181 && x.longitude > -181);
		List<WorldCitiesCordonnees> mydata = rddFiltred.collect();
		
		System.out.println("Nombre de ligne All" + mydata.size());
		
		for (int i = 0; i < mydata.size(); i++) {
			Put put = new Put(Bytes.toBytes("ROW" + i));

			put.addColumn(FAMILY, Bytes.toBytes("pays"), Bytes.toBytes(mydata.get(i).pays));
			put.addColumn(FAMILY, Bytes.toBytes("ville"), Bytes.toBytes(mydata.get(i).ville));
			put.addColumn(FAMILY, Bytes.toBytes("region"), Bytes.toBytes(mydata.get(i).region));
			put.addColumn(FAMILY, Bytes.toBytes("population"), Bytes.toBytes(Double.toString(mydata.get(i).population)));
			put.addColumn(FAMILY, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(mydata.get(i).latitude)));
			put.addColumn(FAMILY, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(mydata.get(i).longitude)));

			table.put(put);
		}
		
		
		Function2<Double, Double, Double> sumFunc = (x, y) -> (x+y);
		
		Function2<Integer, Integer, Integer> nbVille = (x, y) -> (x+y);
		
		Function2<Double, Integer, Double> avg = (x, y) -> (x/y);
	
		// Niveau de zoom0: moyenne de la population par pays:
		//RDD avec les pays et leurs population
		JavaPairRDD<String, Double> population0 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.pays, world.population);
					}
				});
		
		//RDD avec les pays et leurs latitudes
		JavaPairRDD<String, Double> latitude0 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.pays, world.latitude);
					}
				});
	
		//RDD avec les pays et leurs longitudes
		JavaPairRDD<String, Double> longitude0 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.pays, world.longitude);
					}
				});
		
		//RDD avec les pays et des 1 pour compter combien de fois ce pays apparait avec une population
		JavaPairRDD<String, Integer> nb0 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(WorldCitiesCordonnees world) throws Exception {	
						return new Tuple2<String, Integer>(world.pays, 1);
					}
				});
				
		//Population de chaque pays
		JavaPairRDD<String, Double> sum = population0.reduceByKey(sumFunc);
		//Nombre d'apparition d'un pays
		//Sum Latitude de chaque pays
		JavaPairRDD<String, Double> sumLat = latitude0.reduceByKey(sumFunc);
		//Sum Mongitude de chaque pays
		JavaPairRDD<String, Double> sumLong = longitude0.reduceByKey(sumFunc);
		//Nombre d'apparition d'un pays
		JavaPairRDD<String, Integer> nb = nb0.reduceByKey(nbVille);
		
		
		//Moyenne de la lalitude d'un pays
		JavaPairRDD<String, Double> avgLatitudeCountry = sumLat.join(nb).mapValues( x-> x._1 / x._2);
		
		//Moyenne de la longitude d'un pays
		JavaPairRDD<String, Double> avgLongitudeCountry = sumLong.join(nb).mapValues( x-> x._1 / x._2);
		
		
		List<Tuple2<String,Double>> avglat = avgLatitudeCountry.collect();
		List<Tuple2<String,Double>> avglong = avgLongitudeCountry.collect();
		List<Tuple2<String,Double>> sumpop = sum.collect();

		System.out.println("	***************		Nombre de ligne Country		****************	"  + sumpop.size());
		for (int i = 0; i < sumpop.size(); i++) {
			Put put = new Put(Bytes.toBytes("ROW" + i));
			
			put.addColumn(FAMILY_Pays, Bytes.toBytes("pays"), Bytes.toBytes(sumpop.get(i)._1));
			put.addColumn(FAMILY_Pays, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(avglat.get(i)._2)));
			put.addColumn(FAMILY_Pays, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(avglong.get(i)._2)));
			put.addColumn(FAMILY_Pays, Bytes.toBytes("population"), Bytes.toBytes(Double.toString(sumpop.get(i)._2)));
			
			tableCountry.put(put);
		}


		// Niveau de zoom1: moyenne de la population par region
		//RDD avec les regions et leurs population
		JavaPairRDD<String, Double> population1 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.region, world.population);
					}
				});
		
		//RDD avec les pays et leurs latitudes
		JavaPairRDD<String, Double> latitude1 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.region, world.latitude);
					}
				});
		
		//RDD avec les pays et leurs longitudes
		JavaPairRDD<String, Double> longitude1 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.region, world.longitude);
					}
				});
		
		//RDD avec les pays et des 1 pour compter combien de fois ce pays apparait avec une population
		JavaPairRDD<String, Integer> nb1 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(WorldCitiesCordonnees world) throws Exception {	
						return new Tuple2<String, Integer>(world.region, 1);
					}
				});
				
		//Population de chaque pays
		JavaPairRDD<String, Double> sum1 = population1.reduceByKey(sumFunc);
		//Nombre d'apparition d'un pays
		//Sum Latitude de chaque pays
		JavaPairRDD<String, Double> sumLat1 = latitude1.reduceByKey(sumFunc);
		//Sum Mongitude de chaque pays
		JavaPairRDD<String, Double> sumLong1 = longitude1.reduceByKey(sumFunc);
		//Nombre d'apparition d'un pays
		JavaPairRDD<String, Integer> nbb = nb1.reduceByKey(nbVille);
		
		
		//Moyenne de la lalitude d'un pays
		JavaPairRDD<String, Double> avgLatitudeRegion = sumLat1.join(nbb).mapValues( x-> x._1 / x._2);
		
		//Moyenne de la longitude d'un pays
		JavaPairRDD<String, Double> avgLongitudeRegion = sumLong1.join(nbb).mapValues( x-> x._1 / x._2);
		
		
		List<Tuple2<String,Double>> avglat1 = avgLatitudeRegion.collect();
		List<Tuple2<String,Double>> avglong1 = avgLongitudeRegion.collect();
		List<Tuple2<String,Double>> sumpop1 = sum1.collect();

		System.out.println("	***************		Nombre de ligne Region		****************	" + sumpop1.size());
		for (int i = 0; i < sumpop1.size(); i++) {
			Put put = new Put(Bytes.toBytes("ROW" + i));
			put.addColumn(FAMILY_Region, Bytes.toBytes("region"), Bytes.toBytes(sumpop1.get(i)._1));
			put.addColumn(FAMILY_Region, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(avglat1.get(i)._2)));
			put.addColumn(FAMILY_Region, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(avglong1.get(i)._2)));
			put.addColumn(FAMILY_Region, Bytes.toBytes("population"), Bytes.toBytes(Double.toString(sumpop1.get(i)._2)));
			
			tableRegion.put(put);
		}

	
		// Niveau de zoom2: moyenne de la population par ville
		//RDD avec les villes et leurs population
		JavaPairRDD<String, Double> population2 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.ville, world.population);
					}
				});
		
		//RDD avec les pays et leurs latitudes
		JavaPairRDD<String, Double> latitude2 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.ville, world.latitude);
					}
				});
		
		//RDD avec les pays et leurs longitudes
		JavaPairRDD<String, Double> longitude2 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Double>() {
					@Override
					public Tuple2<String, Double> call(WorldCitiesCordonnees world) throws Exception {
						return new Tuple2<String, Double>(world.ville, world.longitude);
					}
				});
		
		//RDD avec les pays et des 1 pour compter combien de fois ce pays apparait avec une population
		JavaPairRDD<String, Integer> nb2 = rddFiltred
				.mapToPair(new PairFunction<WorldCitiesCordonnees, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(WorldCitiesCordonnees world) throws Exception {	
						return new Tuple2<String, Integer>(world.ville, 1);
					}
				});
				
		//Population de chaque ville
		JavaPairRDD<String, Double> summm = population2.reduceByKey(sumFunc);
		//Nombre d'apparition d'une ville
		//Sum Latitude de chaque pays
		JavaPairRDD<String, Double> summmLat = latitude2.reduceByKey(sumFunc);
		//Sum Mongitude de chaque ville
		JavaPairRDD<String, Double> summmLong = longitude2.reduceByKey(sumFunc);
		//Nombre d'apparition d'une ville
		JavaPairRDD<String, Integer> nbbb = nb2.reduceByKey(nbVille);
		
		
		//Moyenne de la lalitude d'un pays
		JavaPairRDD<String, Double> avgLatitudeCity = summmLat.join(nbbb).mapValues( x-> x._1 / x._2);
		
		//Moyenne de la longitude d'un pays
		JavaPairRDD<String, Double> avgLongitudeCity = summmLong.join(nbbb).mapValues( x-> x._1 / x._2);
		
		
		List<Tuple2<String,Double>> avglat2 = avgLatitudeCity.collect();
		List<Tuple2<String,Double>> avglong2 = avgLongitudeCity.collect();
		List<Tuple2<String,Double>> sumpop2 = summm.collect();

		System.out.println("	***************		Nombre de ligne Cities		****************	"  + sumpop2.size());
		
		for (int i = 0; i < sumpop2.size(); i++) {
			Put put = new Put(Bytes.toBytes("ROW" + i));
			
			put.addColumn(FAMILY_City, Bytes.toBytes("city"), Bytes.toBytes(sumpop2.get(i)._1));
			put.addColumn(FAMILY_City, Bytes.toBytes("latitude"), Bytes.toBytes(Double.toString(avglat2.get(i)._2)));
			put.addColumn(FAMILY_City, Bytes.toBytes("longitude"), Bytes.toBytes(Double.toString(avglong2.get(i)._2)));
			put.addColumn(FAMILY_City, Bytes.toBytes("population"), Bytes.toBytes(Double.toString(sumpop2.get(i)._2)));
			
			tableCity.put(put);
		}
		
	}

}
