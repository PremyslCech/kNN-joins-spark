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

		public void run(String inputPath) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Duplicate feature detector").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, String.class, Long.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			int numberOfExecutors = jsc.getConf().getInt("spark.executor.instances", -1);

			JavaPairRDD<Long, String> groupedFeatures = getDuplicateFeatures(jsc.textFile(inputPath, numberOfExecutors));

			for (Tuple2<Long, String> groupSize : groupedFeatures.collect()) {
				System.out.println(groupSize._1 + " " + groupSize._2);
			}

			System.out.println("Duplicate feature detection done!");
			jsc.close();
		}

		private JavaPairRDD<Long, String> getDuplicateFeatures(JavaRDD<String> data) {
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
			}).filter(new Function<Tuple2<String, Long>, Boolean>() {
				public Boolean call(Tuple2<String, Long> pair) throws Exception {
					return pair._2 > 1;
				}
			}).mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
				public Tuple2<Long, String> call(Tuple2<String, Long> pair) throws Exception {
					return new Tuple2<>(pair._2, pair._1);
				}
			}).sortByKey(false);
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.out.println("Wrong number of arguments, usage is: <input_path>");
			return;
		}

		Job job = new Job();
		job.run(args[0]);
	}
}
