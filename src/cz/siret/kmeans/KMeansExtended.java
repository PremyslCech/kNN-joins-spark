package cz.siret.kmeans;

import java.io.Serializable;
import java.util.Arrays;

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

public class KMeansExtended {

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
			JavaPairRDD<Integer, ObjectWithVector> parsedData = data.mapToPair(new PairFunction<String, Integer, ObjectWithVector>() {
				public Tuple2<Integer, ObjectWithVector> call(String s) {

					s = s.replace(',', '.');
					String[] parts = s.split(";");
					double[] values = new double[parts.length - 1];
					for (int i = 0; i < values.length; i++) {
						values[i] = Double.parseDouble(parts[i + 1]);
					}
					int indexOf = parts[0].indexOf('|');
					int id = Integer.valueOf(parts[0].substring(0, indexOf));
					return new Tuple2<>(id, new ObjectWithVector(id, parts[0].substring(indexOf), Vectors.dense(values)));
				}
			});

			RDD<Vector> vectors = parsedData.map(new Function<Tuple2<Integer, ObjectWithVector>, Vector>() {
				public Vector call(Tuple2<Integer, ObjectWithVector> tuple) throws Exception {
					return tuple._2.getVector();
				}
			}).repartition(numberOfExecutors).rdd().cache();

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

			JavaPairRDD<Integer, ObjectWithCentroid> objectsWithCentroids = parsedData
					.mapToPair(new PairFunction<Tuple2<Integer, ObjectWithVector>, Integer, ObjectWithCentroid>() {

						KMeansModel clusters = broadcastedClusters.value();

						public Tuple2<Integer, ObjectWithCentroid> call(Tuple2<Integer, ObjectWithVector> object) throws Exception {

							int nearestCluster = clusters.predict(object._2.getVector());
							return new Tuple2<>(object._1, new ObjectWithCentroid(object._1 + object._2.getCoordinates(), nearestCluster));
						}
					}).cache();

			objectsWithCentroids.values().saveAsTextFile(outputPath + "/objects_centroids");

			objectsWithCentroids.groupByKey().map(new Function<Tuple2<Integer, Iterable<ObjectWithCentroid>>, ObjectWithHistogram>() {
				public ObjectWithHistogram call(Tuple2<Integer, Iterable<ObjectWithCentroid>> object) throws Exception {

					float[] histogram = new float[clusterCount];
					for (ObjectWithCentroid obj : object._2) {
						int nearestCluster = obj.getCentroidId();
						histogram[nearestCluster]++;
					}
					return new ObjectWithHistogram(object._1.toString(), histogram);
				}
			}).saveAsTextFile(outputPath + "/histograms");

			jsc.parallelize(Arrays.asList(clusters.clusterCenters())).saveAsTextFile(outputPath + "/centroids");

			System.out.println("K-Means finished.");

			jsc.close();
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.out.println("Wrong number of arguments, usage is: <data> <output_folder> <cluster_count> <iterations_count>");
			return;
		}

		Job job = new Job(args);
		job.run();
	}
}
