package cz.siret.kmeans;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import cz.siret.knn.SparkUtils;
import scala.Tuple2;

public class KMeans {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final String inputData;
		private final String outputPath;

		private final int clusterCount;
		private final int iterationCount;

		public Job(String[] args) {

			inputData = args[0];
			outputPath = args[1];
			clusterCount = Integer.valueOf(args[2]);
			iterationCount = Integer.valueOf(args[3]);
		}

		public void run() throws Exception {

			SparkConf conf = new SparkConf().setAppName("KMeans").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] {});
			JavaSparkContext jsc = new JavaSparkContext(conf);

			int numberOfExecutors = conf.getInt("spark.executor.instances", -1);

			// delete output directory if exists
			new SparkUtils().deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			System.out.println("Startup ready. Number of executors: " + numberOfExecutors);

			JavaRDD<String> data = jsc.textFile(inputData);
			JavaPairRDD<Integer, Vector> parsedData = data.mapToPair(new PairFunction<String, Integer, Vector>() {
				public Tuple2<Integer, Vector> call(String s) {
					
					s = s.replace(',', '.');
					String[] parts = s.split(";");
					double[] values = new double[parts.length - 1];
					for (int i = 0; i < values.length; i++) {
						values[i] = Double.parseDouble(parts[i + 1]);
					}
					return new Tuple2<>(Integer.valueOf(parts[0]), Vectors.dense(values));
				}
			});

			RDD<Vector> vectors = parsedData.values().repartition(numberOfExecutors).rdd().cache();

			System.out.println("Training K-Means.");
			long begin = System.currentTimeMillis();
			// cluster the data into classes using KMeans
			KMeansModel clusters = org.apache.spark.mllib.clustering.KMeans.train(vectors, clusterCount, iterationCount);
			long end = System.currentTimeMillis();
			System.out.println("Done. Time: " + (end - begin) / 1000d);

			// Evaluate clustering by computing Within Set Sum of Squared Errors
			double WSSSE = clusters.computeCost(vectors);
			System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

			final Broadcast<KMeansModel> broadcastedClusters = jsc.broadcast(clusters);

			JavaRDD<ObjectWithHistogram> bowResult = parsedData.groupByKey().map(new Function<Tuple2<Integer, Iterable<Vector>>, ObjectWithHistogram>() {

				KMeansModel clusters = broadcastedClusters.value();

				public ObjectWithHistogram call(Tuple2<Integer, Iterable<Vector>> object) throws Exception {

					float[] histogram = new float[clusters.k()];
					for (Vector vector : object._2) {
						int nearestCluster = clusters.predict(vector);
						histogram[nearestCluster]++;
					}
					return new ObjectWithHistogram(object._1.toString(), histogram);
				}
			});
			bowResult.saveAsTextFile(outputPath);

			System.out.println("K-Means finished.");

			// System.out.println("Cluster centers:");
			// for (Vector center : clusters.clusterCenters()) {
			// System.out.println(" " + center);
			// }

			// Save and load model
			// clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
			// KMeansModel sameModel = KMeansModel.load(jsc.sc(),
			// "target/org/apache/spark/JavaKMeansExample/KMeansModel");

			jsc.close();
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.out.println("Wrong number of arguments, usage is: <data> <output_clusters> <cluster_count> <iterations_count>");
			return;
		}

		Job job = new Job(args);
		job.run();
	}
}
