package cz.siret.knn.curve;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import cz.siret.knn.KnnHelper;
import cz.siret.knn.KnnMetrics;
import cz.siret.knn.Logger;
import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.SiretConfig;
import cz.siret.knn.SparkMetricsRegistrator;
import cz.siret.knn.SparkUtils;
import cz.siret.knn.metric.IMetric;
import cz.siret.knn.metric.L2MetricSiret;
import cz.siret.knn.metric.L2Naive;
import cz.siret.knn.model.*;
import cz.siret.knn.model.Feature.DimValue;
import cz.siret.knn.naive.NaiveKnnCalculator;
import scala.Tuple2;

/**
 * Curve (actually Z-curve) approximate kNN join
 * 
 * @author pcech
 *
 */
public class KNNJoinApprox {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final int SEED = 937;

		private final String databasePath;
		private final String queryPath;

		private final int k;
		private final int minNumberOfPartitions;
		private final int reducers;

		private final int shifts;
		private final int shiftsRange;
		private final int shiftsScale;
		private final double samplingRate;
		// determines if the kNN for a query is computed for all DB objects in a partition or just for "k" lower and higher z-values near a query z-value
		private final boolean considerEntireBuckets;
		// if true saves original features with z-order - way more memory demanding but faster
		// if false saves only z-order - coordinates are obtained back from the z-order - slower but uses way less memory
		private final boolean storeOnlyZorder;

		public Job(String[] args) {

			databasePath = args[0];
			queryPath = args[1];

			// parsing parameters
			SparkConf conf = new SparkConf();
			reducers = conf.getInt(SiretConfig.REDUCERS_COUNT_STR, 10);
			minNumberOfPartitions = conf.getInt(SiretConfig.MIN_PARITTIONS_COUNT_STR, 10);
			k = conf.getInt(SiretConfig.K_STR, 10);

			shifts = conf.getInt(SiretConfig.SHIFTS_STR, 2);
			shiftsRange = conf.getInt(SiretConfig.CURVE_RANGE_STR, 500 * 1000 * 1000);
			shiftsScale = conf.getInt(SiretConfig.CURVE_SCALE_STR, 1000 * 1000 * 1000);
			samplingRate = conf.getDouble(SiretConfig.SAMPLING_RATE_STR, 0.001);
			considerEntireBuckets = conf.getBoolean(SiretConfig.CONSIDER_ENTIRE_BUCKETS_STR, true);
			storeOnlyZorder = conf.getBoolean(SiretConfig.ONLY_Z_ORDER_STR, false);
		}

		public void run(String outputPath) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Curve kNN join").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, ICurveOrder.class, CurveWithFeature.class, Curve.class,
							BigInteger.class, PartitionWithShift.class, Partitions.class, PartitionBoundaries.class, ObjectWithDistance.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			KnnMetrics knnMetrics = SparkMetricsRegistrator.register(jsc.sc());
			Logger logger = new Logger(jsc.getConf().get("spark.submit.deployMode").toLowerCase().equals("client")); // logging only for the client mode
			SparkUtils sparkUtils = new SparkUtils();
			sparkUtils.deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			JavaRDD<Feature> databaseFeatures = sparkUtils.readInputPath(jsc, databasePath, minNumberOfPartitions);
			JavaRDD<Feature> queriesFeatures = sparkUtils.readInputPath(jsc, queryPath, minNumberOfPartitions).cache();

			double dbProbability = samplingRate;
			double queryProbability = samplingRate;

			int dimension = getDimension(queriesFeatures);
			printParameters(jsc.getConf().get("spark.serializer"), dimension, dbProbability, queryProbability);

			int[][] randomShifts = genRandomShiftVectors(shifts, dimension, shiftsRange);
			Broadcast<int[][]> broadcastedShifts = jsc.broadcast(randomShifts);

			JavaPairRDD<Integer, ICurveOrder> databaseWithCurve = mapFeaturesToCurve(databaseFeatures, broadcastedShifts, shiftsScale, dimension, storeOnlyZorder)
					.persist(StorageLevel.MEMORY_AND_DISK_SER());
			JavaPairRDD<Integer, ICurveOrder> queriesWithCurve = mapFeaturesToCurve(queriesFeatures, broadcastedShifts, shiftsScale, dimension, storeOnlyZorder)
					.persist(StorageLevel.MEMORY_AND_DISK_SER());

			// Partitioning
			Partitions partitions = new CurvePartitioner().getPartitions(queriesWithCurve, databaseWithCurve, dbProbability, queryProbability, k, reducers, dimension,
					shifts);
			Broadcast<Partitions> broadcastedPartitions = jsc.broadcast(partitions);
			logger.logCurvePartitions(partitions);
			System.out.println("Partitioning done.");
			//

			// kNN computation
			JavaRDD<NNResult> kNNResult;
			if (considerEntireBuckets && !storeOnlyZorder) { // the naive kNN join is performed for each partition
				JavaPairRDD<Iterable<Feature>, Iterable<Feature>> partitionsForKnn = getPartitionsForKnnComputation(queriesWithCurve, databaseWithCurve,
						broadcastedPartitions, reducers);

				kNNResult = new NaiveKnnCalculator().calculateKnn(partitionsForKnn, k);
			} else { // candidate neighbors are computed only for "k" lower and higher z-values
				JavaPairRDD<FeaturesKey, NNResult> intermediateKnnResults = getIntermediateKnnResults(queriesWithCurve, databaseWithCurve,
						broadcastedPartitions, reducers, k, storeOnlyZorder, shiftsScale);
				kNNResult = new NaiveKnnCalculator().mergeResults(intermediateKnnResults, k);
			}
			//
			kNNResult.saveAsTextFile(outputPath);
			System.out.println("Curve kNN join done!");
			System.out.println(knnMetrics.produceOutput());

			jsc.close();
		}

		private int getDimension(JavaRDD<Feature> queriesFeatures) {

			int[] histogramKeys = queriesFeatures.take(1).get(0).getKeys();
			return histogramKeys[histogramKeys.length - 1] + 1;
		}

		private void printParameters(String serializer, int dimension, double dbProbability, double queryProbability) {
			System.out.println("Starting computing curve kNN join. Parameters: ");
			System.out.println("Startup ready. Serializer: " + serializer);
			System.out.println("K: " + k + ", Reducers: " + reducers + ", Dimensions: " + dimension + ", Shifts: " + shifts + ", ShiftsRange: " + shiftsRange
					+ ", ShiftsScale: " + shiftsScale + ", SamplingRate: " + samplingRate + ", EntireBuckets: " + considerEntireBuckets);
		}

		private int[][] genRandomShiftVectors(int shifts, int dimension, int shiftsRange) throws IOException {

			Random r = new Random(SEED);
			int[][] shiftvectors = new int[shifts][];

			// Generate random shift vectors
			for (int i = 0; i < shifts; i++) {
				shiftvectors[i] = ZorderHelper.createShift(dimension, r, i != 0, shiftsRange);
			}
			return shiftvectors;
		}

		private JavaPairRDD<Integer, ICurveOrder> mapFeaturesToCurve(JavaRDD<Feature> features, final Broadcast<int[][]> shifts, final int shiftsScale,
				final int dimension, final boolean storeOnlyZorder) {

			return features.flatMapToPair(new PairFlatMapFunction<Feature, Integer, ICurveOrder>() {
				public Iterator<Tuple2<Integer, ICurveOrder>> call(Feature feature) throws Exception {

					int[][] shiftsLocal = shifts.value();
					List<Tuple2<Integer, ICurveOrder>> output = new ArrayList<>();
					// for number of shifts
					for (int i = 0; i < shiftsLocal.length; i++) {

						int[] coordinates = toCoordinates(feature, shiftsLocal[i]);
						BigInteger zorder = ZorderHelper.valueOf(coordinates);
						ICurveOrder curveWithFeature = storeOnlyZorder ? new Curve(zorder, feature.toFeaturesKeyClassified(), dimension)
								: new CurveWithFeature(zorder, feature);
						output.add(new Tuple2<>(i, curveWithFeature));
					}
					return output.iterator();
				}

				private int[] toCoordinates(Feature feature, int[] shiftVector) {

					int[] coord = new int[dimension];
					for (DimValue dv : feature.iterate(dimension)) {
						coord[dv.dim] = getShiftedValue(dv.value, shiftVector[dv.dim]);
					}
					return coord;
				}

				private int getShiftedValue(float value, int shift) {
					return ((int) (shiftsScale * value)) + shift;
				}
			});
		}

		private JavaPairRDD<Iterable<Feature>, Iterable<Feature>> getPartitionsForKnnComputation(JavaPairRDD<Integer, ICurveOrder> queries,
				JavaPairRDD<Integer, ICurveOrder> database, final Broadcast<Partitions> partitions, int reducers) {

			Function<ICurveOrder, Feature> selector = new Function<ICurveOrder, Feature>() {
				public Feature call(ICurveOrder curveWithFeature) {
					return (Feature) curveWithFeature.getFeature();
				}
			};
			return JavaPairRDD.fromJavaRDD(queries.flatMapToPair(getPartitionFunction(partitions, false, selector))
					.cogroup(database.flatMapToPair(getPartitionFunction(partitions, true, selector)), reducers).values());
		}

		private <T> PairFlatMapFunction<Tuple2<Integer, ICurveOrder>, PartitionWithShift, T> getPartitionFunction(final Broadcast<Partitions> partitions,
				final boolean isDatabase, final Function<ICurveOrder, T> selector) {

			return new PairFlatMapFunction<Tuple2<Integer, ICurveOrder>, PartitionWithShift, T>() {
				public Iterator<Tuple2<PartitionWithShift, T>> call(Tuple2<Integer, ICurveOrder> featureWithCurve) throws Exception {

					Partitions partitionsLocal = partitions.value();
					List<PartitionBoundaries> partitionsForShift = (isDatabase ? partitionsLocal.getDbBoundaries() : partitionsLocal.getQueryBoundaries())
							.get(featureWithCurve._1);

					List<Tuple2<PartitionWithShift, T>> output = new ArrayList<>();
					BigInteger curveValue = featureWithCurve._2.getCurveOrder();

					int partitionId = 0;
					// TODO maybe take advantage of sorted order - but for DB the partitions can overlap
					for (PartitionBoundaries partitionBoundaries : partitionsForShift) {

						// "start <= curveValue && curveValue <= end"
						if (partitionBoundaries.getStart().compareTo(curveValue) <= 0 && curveValue.compareTo(partitionBoundaries.getEnd()) <= 0) {
							output.add(new Tuple2<>(new PartitionWithShift(partitionId, featureWithCurve._1), selector.call(featureWithCurve._2)));
						}

						partitionId++;
					}

					return output.iterator();
				}
			};
		}

		private JavaPairRDD<FeaturesKey, NNResult> getIntermediateKnnResults(JavaPairRDD<Integer, ICurveOrder> queries,
				JavaPairRDD<Integer, ICurveOrder> database, final Broadcast<Partitions> partitions, int reducers, final int k, final boolean storeOnlyZorder,
				final int shiftsScale) {

			Function<ICurveOrder, ICurveOrder> selector = new Function<ICurveOrder, ICurveOrder>() {
				public ICurveOrder call(ICurveOrder curveWithFeature) {
					return curveWithFeature;
				}
			};

			return queries.flatMapToPair(getPartitionFunction(partitions, false, selector))
					.cogroup(database.flatMapToPair(getPartitionFunction(partitions, true, selector)), reducers).flatMapToPair(
							new PairFlatMapFunction<Tuple2<PartitionWithShift, Tuple2<Iterable<ICurveOrder>, Iterable<ICurveOrder>>>, FeaturesKey, NNResult>() {

								public Iterator<Tuple2<FeaturesKey, NNResult>> call(
										final Tuple2<PartitionWithShift, Tuple2<Iterable<ICurveOrder>, Iterable<ICurveOrder>>> partition) throws Exception {

									return new Iterator<Tuple2<FeaturesKey, NNResult>>() {

										private final KnnHelper knnHelper = new KnnHelper();
										private final List<ICurveOrder> databaseObjects = new ArrayList<>();
										private final Iterator<ICurveOrder> queryIterator = partition._2._1.iterator();
										private final PriorityQueue<Tuple2<FeaturesKeyClassified, Float>> knn;
										{
											knn = knnHelper.getNeighborsPriorityQueue(k);
											for (ICurveOrder feature : partition._2._2) {
												databaseObjects.add(feature);
											}
											if (databaseObjects.size() < 2 * k) {
												throw new Error("Size of the database is less then 2k (k = " + k + "), DB objects size: " + databaseObjects.size());
											}
											Collections.sort(databaseObjects);
										}

										@Override
										public boolean hasNext() {
											return queryIterator.hasNext();
										}

										@Override
										public Tuple2<FeaturesKey, NNResult> next() {

											final IMetric metric = storeOnlyZorder ? new L2Naive(shiftsScale) : new L2MetricSiret();
											knn.clear();
											ICurveOrder queryWithCurve = queryIterator.next();

											int place = Collections.binarySearch(databaseObjects, queryWithCurve);
											if (place < 0) {
												place = -(place + 1); // if < 0 its -(insert place) - 1
											}

											int low = place - k; // index included
											int high = place + k; // index excluded
											if (high > databaseObjects.size()) {
												low -= high - databaseObjects.size();
												high -= high - databaseObjects.size();
											} else if (low < 0) {
												high -= low;
												low = 0;
											}

											Object query = queryWithCurve.getFeature();
											for (int i = low; i < high; i++) {

												ICurveOrder databaseObject = databaseObjects.get(i);
												try {
													knn.add(new Tuple2<>(databaseObject.getFeatureKey(), metric.dist(query, databaseObject.getFeature())));
												} catch (IOException e) {
													e.printStackTrace();
												}
												if (knn.size() > k) {
													knn.remove();
												}
											}
											return new Tuple2<>(new FeaturesKey(queryWithCurve.getFeatureKey()),
													knnHelper.getNeighborsResult(queryWithCurve.getFeatureKey(), knn));
										}
									};
								}
							});
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Wrong number of arguments, usage is: <database> <queries> <output>");
			return;
		}

		Job job = new Job(args);
		job.run(args[2]);
	}
}
