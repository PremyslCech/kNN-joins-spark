package cz.siret.knn.naive;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.SparkUtils;
import cz.siret.knn.model.*;
import cz.siret.knn.pivot.FeatureWithMultiplePartitions;
import cz.siret.knn.pivot.FeatureWithOnePartition;
import cz.siret.knn.pivot.PartitionStatistics;
import cz.siret.knn.pivot.VoronoiStatistics;
import scala.Tuple2;
import java.lang.Exception;
import java.util.Random;

public class NaiveKNNJoin {

	/**
	 * Main execution of the whole job. Must be serializable because Spark requires it.
	 */
	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final String databasePath;
		private final String queriesPath;
		private final String outputPath;

		private final int k;
		private final int numberOfPartitions;

		public Job(String[] args) {

			databasePath = args[0];
			queriesPath = args[1];
			outputPath = args[2];
			k = Integer.parseInt(args[3]);
			numberOfPartitions = Integer.parseInt(args[4]);
		}

		public void run() throws Exception {

			System.out.println("Starting Spark with parameters:");
			System.out.println("K: " + k + ", Number of partitions: " + numberOfPartitions);

			SparkConf conf = new SparkConf().setAppName("Naive kNN join").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, Pair.class, FeatureWithOnePartition.class,
							FeatureWithMultiplePartitions.class, ObjectWithDistance.class, VoronoiStatistics.class, PartitionStatistics.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			SparkUtils sparkUtils = new SparkUtils();
			System.out.println("Serializer: " + jsc.getConf().get("spark.serializer"));

			// delete output directory if exists
			sparkUtils.deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			JavaRDD<Feature> databaseFeatures = sparkUtils.readInputPath(jsc, databasePath, 1);
			JavaRDD<Feature> queryFeatures = sparkUtils.readInputPath(jsc, queriesPath, 1);

			JavaRDD<NNResult> kNNResult = getKnnResults(databaseFeatures, queryFeatures, k);

			kNNResult.saveAsTextFile(outputPath);
			System.out.println("The kNN job has finished.");
			// System.out.println("Distance computations: " + metric.numOfDistComp);

			jsc.close();
		}

		private JavaRDD<NNResult> getKnnResults(JavaRDD<Feature> databaseFeatures, JavaRDD<Feature> queryFeatures, int k) {

			NaiveKnnCalculator knnCalculator = new NaiveKnnCalculator();
			return knnCalculator.calculateKnn(queryFeatures.mapToPair(getPartitionFunction()).groupByKey(numberOfPartitions).values()
					.cartesian(databaseFeatures.mapToPair(getPartitionFunction()).groupByKey(numberOfPartitions).values()), k);
		}

		private PairFunction<Feature, Integer, Feature> getPartitionFunction() {
			return new PairFunction<Feature, Integer, Feature>() {

				public Tuple2<Integer, Feature> call(Feature feature) throws Exception {
					return new Tuple2<>(new Random().nextInt(numberOfPartitions), feature);
				}
			};
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			System.out.println("Wrong number of arguments, usage is: <database> <queries> <output> <K> <numberOfPartitions>");
			return;
		}

		Job job = new Job(args);
		job.run();
	}

}
