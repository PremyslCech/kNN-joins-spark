package cz.siret.knn.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import cz.siret.knn.SparkUtils;
import cz.siret.knn.metric.MetricProvider;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;
import scala.Tuple2;

/**
 * Gets some data statistics
 * 
 * @author pcech
 *
 */
public class DistanceDistribution {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		public void run(String inputPath, double fraction) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Distance distribution").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			int numberOfExecutors = jsc.getConf().getInt("spark.executor.instances", -1);

			SparkUtils sparkUtils = new SparkUtils();

			JavaRDD<Feature> features = sparkUtils.readInputPath(jsc, inputPath, numberOfExecutors);
			List<Tuple2<Float, Long>> histogram = getDistanceDistributionHistogram(jsc, features, fraction);

			for (Tuple2<Float, Long> tuple : histogram) {
				System.out.println(tuple._1 + "," + tuple._2);
			}
			System.out.println("Distance distribution done!");

			jsc.close();
		}

		private List<Tuple2<Float, Long>> getDistanceDistributionHistogram(JavaSparkContext jsc, JavaRDD<Feature> features, double fraction) {

			final int seed = 937;
			final int numberOfBins = 500;

			JavaPairRDD<Feature, Long> sample = features.sample(false, fraction, seed).zipWithIndex().cache();
			System.out.println("Sample count: " + sample.count());

			JavaRDD<Float> distances = sample.cartesian(sample).map(new Function<Tuple2<Tuple2<Feature, Long>, Tuple2<Feature, Long>>, Float>() {
				public Float call(Tuple2<Tuple2<Feature, Long>, Tuple2<Feature, Long>> pair) throws Exception {
					return pair._1._2 <= pair._2._2 ? null : MetricProvider.getMetric().dist(pair._1._1, pair._2._1);
				}
			}).filter(new Function<Float, Boolean>() {
				public Boolean call(Float dist) throws Exception {
					return dist != null;
				}
			}).persist(StorageLevel.MEMORY_AND_DISK());

			float min = distances.reduce(new Function2<Float, Float, Float>() {
				public Float call(Float arg0, Float arg1) throws Exception {
					return Math.min(arg0, arg1);
				}
			});
			float max = distances.reduce(new Function2<Float, Float, Float>() {
				public Float call(Float arg0, Float arg1) throws Exception {
					return Math.max(arg0, arg1);
				}
			});
			System.out.println("Min distance: " + min);
			System.out.println("Max distance: " + max);

			final Broadcast<float[]> broadcastedSteps = jsc.broadcast(getSteps(numberOfBins, min, max));

			return distances.mapToPair(new PairFunction<Float, Float, Long>() {
				public Tuple2<Float, Long> call(Float distance) throws Exception {

					float[] stepsLocal = broadcastedSteps.value();
					int index = Arrays.binarySearch(stepsLocal, distance);
					if (index < 0) {
						index = -(index + 1);
						// first or last position
						if (index == stepsLocal.length || index != 0 && Math.abs(stepsLocal[index - 1] - distance) < Math.abs(stepsLocal[index] - distance)) {
							index--;
						}
					}
					return new Tuple2<>(stepsLocal[index], 1L);
				}
			}).reduceByKey(new Function2<Long, Long, Long>() {
				public Long call(Long arg0, Long arg1) throws Exception {
					return arg0 + arg1;
				}
			}).sortByKey().collect();
		}

		private float[] getSteps(int numberOfBins, float min, float max) {

			float[] output = new float[numberOfBins];
			float step = (max - min) / numberOfBins;
			for (int i = 0; i < output.length; i++) {
				output[i] = min + i * step;
			}
			return output;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Wrong number of arguments, usage is: <input_path> <fraction>");
			return;
		}

		Job job = new Job();
		job.run(args[0], Double.valueOf(args[1]));
	}
}
