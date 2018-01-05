package cz.siret.knn.pivot.stats;

import java.io.FileWriter;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.SiretConfig;
import cz.siret.knn.model.*;
import cz.siret.knn.pivot.FeatureWithMultiplePartitions;
import cz.siret.knn.pivot.FeatureWithOnePartition;
import cz.siret.knn.pivot.KNNJoinApprox;
import cz.siret.knn.pivot.PartitionStatistics;
import cz.siret.knn.pivot.VoronoiStatistics;
import cz.siret.knn.pivot.KNNJoinApprox.Method;
import scala.Tuple2;

public class StatisticsCalculator {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final KNNJoinApprox.Job knnJob;

		private final String statsFile;

		private final Method[] methods;
		private final int[] kValues;
		private final int[] pivotCounts;
		private final int[] maxRecDepths;
		private final int[] maxReducers;
		private final float[] filterRatios;

		public Job(String[] args) {

			knnJob = new KNNJoinApprox.Job(args);
			statsFile = args[2];

			// parsing parameters
			SparkConf conf = new SparkConf();
			methods = parseMethodsParameters(conf.get(SiretConfig.METHODS_STR, joinArrayValues(Method.values(), ';')));
			kValues = parseIntParameters(conf.get(SiretConfig.KVALUES_STR));
			pivotCounts = parseIntParameters(conf.get(SiretConfig.PIVOTCOUNTS_STR));
			maxRecDepths = parseIntParameters(conf.get(SiretConfig.MAX_REC_DEPTHS_STR));
			maxReducers = parseIntParameters(conf.get(SiretConfig.MAX_REDUCERS_STR));
			filterRatios = parseFloatParameters(conf.get(SiretConfig.FILTER_RATIOS_STR));
		}

		public void run() throws Exception {

			SparkConf conf = new SparkConf().setAppName("Pivot kNN join statistics").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, Pair.class, FeatureWithOnePartition.class,
							FeatureWithMultiplePartitions.class, ObjectWithDistance.class, VoronoiStatistics.class, PartitionStatistics.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);

			printParameters();

			FileWriter writer = new FileWriter(statsFile, true);
			LongAccumulator distanceComputations = jsc.sc().longAccumulator();
			LongAccumulator databaseReplications = jsc.sc().longAccumulator();

			for (int pivotCount : pivotCounts)
				for (int k : kValues)
					for (int numberOfReducers : maxReducers)
						for (Method method : methods) { // could be moved even after k values, is here just for correctness check

							float averageDist = -1, maxDist = -1;
							double kNNJoinTimeInSeconds = -1;
							boolean onceComputed = false;

							for (int maxRecDepth : maxRecDepths)
								for (float filterRatio : filterRatios) {

									if ((method != Method.EXACT && method != Method.EXACT_BOUNDS) || !onceComputed) {
										long begin = System.currentTimeMillis();
										try {
											JavaRDD<NNResult> knn = knnJob.computeKnn(jsc, k, method, pivotCount, maxRecDepth, numberOfReducers,
													(int) (pivotCount * filterRatio), distanceComputations, databaseReplications).persist(StorageLevel.MEMORY_AND_DISK_SER());
											maxDist = getMaxDistance(knn, k);
											averageDist = getAverageDistance(knn, k);
										} catch (Exception e) {
											System.err.println(e);
											averageDist = maxDist = Float.NaN;
										}
										long end = System.currentTimeMillis();
										kNNJoinTimeInSeconds = (end - begin) / 1000d;
										onceComputed = true;
									}

									writer.write(produceOutputString("AVG", String.valueOf(averageDist), String.valueOf(method), String.valueOf(k), String.valueOf(maxRecDepth),
											String.valueOf(filterRatio), String.valueOf(pivotCount), String.valueOf(numberOfReducers), String.valueOf(kNNJoinTimeInSeconds)));
									writer.write(System.lineSeparator());
									writer.write(produceOutputString("MAX", String.valueOf(maxDist), String.valueOf(method), String.valueOf(k), String.valueOf(maxRecDepth),
											String.valueOf(filterRatio), String.valueOf(pivotCount), String.valueOf(numberOfReducers), String.valueOf(kNNJoinTimeInSeconds)));
									writer.write(System.lineSeparator());
									writer.flush();
								}
						}

			writer.close();
			jsc.close();
		}

		private void printParameters() {
			System.out.println("Starting calculating knnJoin statistics..");
			System.out.println("Methods:\t\t\t" + joinArrayValues(methods, ' '));
			System.out.println("K Values:\t\t\t" + joinArrayValues(kValues));
			System.out.println("Pivot counts:\t\t\t" + joinArrayValues(pivotCounts));
			System.out.println("Max recursion depths:\t\t" + joinArrayValues(maxRecDepths));
			System.out.println("Max reducers:\t\t\t" + joinArrayValues(maxReducers));
			System.out.println("Filter ratios:\t\t\t" + joinArrayValues(filterRatios));
		}

		private String joinArrayValues(Method[] array, Character separator) {
			StringBuilder output = new StringBuilder();
			for (Method method : array) {
				output.append(method);
				output.append(separator);
			}
			output.deleteCharAt(output.length() - 1);
			return output.toString();
		}

		private String joinArrayValues(int[] array) {
			StringBuilder output = new StringBuilder();
			for (int i : array) {
				output.append(i);
				output.append(" ");
			}
			output.deleteCharAt(output.length() - 1);
			return output.toString();
		}

		private String joinArrayValues(float[] array) {
			StringBuilder output = new StringBuilder();
			for (float value : array) {
				output.append(value);
				output.append(" ");
			}
			output.deleteCharAt(output.length() - 1);
			return output.toString();
		}

		private Method[] parseMethodsParameters(String parameterValues) {
			String[] values = parameterValues.split(SiretConfig.FIELD_SEPARATOR_STR);
			Method[] result = new Method[values.length];
			for (int i = 0; i < values.length; i++) {
				result[i] = Method.valueOf(values[i]);
			}
			return result;
		}

		private int[] parseIntParameters(String parameterValues) {
			String[] values = parameterValues.split(SiretConfig.FIELD_SEPARATOR_STR);
			int[] result = new int[values.length];
			for (int i = 0; i < values.length; i++) {
				result[i] = Integer.valueOf(values[i]);
			}
			return result;
		}

		private float[] parseFloatParameters(String parameterValues) {
			String[] values = parameterValues.split(SiretConfig.FIELD_SEPARATOR_STR);
			float[] result = new float[values.length];
			for (int i = 0; i < values.length; i++) {
				result[i] = Float.valueOf(values[i]);
			}
			return result;
		}

		private float getAverageDistance(JavaRDD<NNResult> knn, final int k) {

			Tuple2<Float, Integer> reduce = knn.map(new Function<NNResult, Tuple2<Float, Integer>>() {
				public Tuple2<Float, Integer> call(NNResult neighbor) throws Exception {
					return new Tuple2<>(neighbor.dbDistances.get(k - 1), 1);
				}
			}).reduce(new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
				public Tuple2<Float, Integer> call(Tuple2<Float, Integer> t1, Tuple2<Float, Integer> t2) throws Exception {
					return new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2);
				}
			});

			return reduce._1 / reduce._2;
		}

		private float getMaxDistance(JavaRDD<NNResult> knn, final int k) {

			return knn.map(new Function<NNResult, Float>() {
				public Float call(NNResult neighbor) throws Exception {
					return neighbor.dbDistances.get(k - 1);
				}
			}).reduce(new Function2<Float, Float, Float>() { // maximum
				public Float call(Float dist1, Float dist2) throws Exception {
					return dist1 > dist2 ? dist1 : dist2;
				}
			});
		}

		private String produceOutputString(String... params) {

			StringBuilder output = new StringBuilder();
			for (String string : params) {
				output.append(string);
				output.append(",");
			}
			output.deleteCharAt(output.length() - 1);
			return output.toString();
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Wrong number of arguments, usage is: <database> <queries> <stats_file>");
			return;
		}

		Job job = new Job(args);
		job.run();
	}
}
