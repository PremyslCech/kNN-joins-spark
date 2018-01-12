package cz.siret.knn.utils;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;
import scala.Tuple2;

/**
 * Gets some data statistics
 * 
 * @author pcech
 *
 */
public class DuplicateFeatureDetector {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		public void run(String queriesPath, String databasePath) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Duplicate feature detector").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, String.class, Long.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			int numberOfExecutors = jsc.getConf().getInt("spark.executor.instances", -1);

			JavaPairRDD<Long, Tuple2<Long, String>> groupedFeatures = getDuplicateFeatures(jsc.textFile(queriesPath, numberOfExecutors / 2),
					jsc.textFile(databasePath, numberOfExecutors / 2));

			for (Tuple2<Long, Tuple2<Long, String>> groupSize : groupedFeatures.collect()) {
				System.out.println(groupSize._1 + " " + groupSize._2._1 + " " + groupSize._2._2);
			}

			System.out.println("Duplicate feature detection done!");
			jsc.close();
		}

		private JavaPairRDD<Long, Tuple2<Long, String>> getDuplicateFeatures(JavaRDD<String> queries, JavaRDD<String> database) {

			// for each query we compute number of duplicates in database
			return getDuplicateObjects(queries).join(getDuplicateObjects(database).filter(new Function<Tuple2<String, Long>, Boolean>() {
				public Boolean call(Tuple2<String, Long> pair) throws Exception {
					return pair._2 > 1;
				}
			})).mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Long>>, Long, Tuple2<Long, String>>() {
				public Tuple2<Long, Tuple2<Long, String>> call(Tuple2<String, Tuple2<Long, Long>> pair) throws Exception {

					long dbDuplicates = pair._2._2;
					long queryDuplicates = pair._2._1;
					return new Tuple2<>(dbDuplicates, new Tuple2<>(queryDuplicates, pair._1));
				}
			}).sortByKey(false);
		}

		private JavaPairRDD<String, Long> getDuplicateObjects(JavaRDD<String> data) {
			return data.mapToPair(new PairFunction<String, String, Long>() {
				public Tuple2<String, Long> call(String line) throws Exception {
					int secondSemicolon = line.indexOf(';', line.indexOf(';') + 1);
					if (secondSemicolon < 0) {
						throw new Exception("Incompatible feature string: " + line);
					}
					return new Tuple2<>(line.substring(secondSemicolon + 1, line.length()), 1L);
				}
			}).reduceByKey(new Function2<Long, Long, Long>() {
				public Long call(Long arg0, Long arg1) throws Exception {
					return arg0 + arg1;
				}
			});
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Wrong number of arguments, usage is: <queries_path> <database_path>");
			return;
		}

		Job job = new Job();
		job.run(args[0], args[1]);
	}
}
