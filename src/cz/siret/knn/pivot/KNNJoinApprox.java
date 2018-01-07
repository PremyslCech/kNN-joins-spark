package cz.siret.knn.pivot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Vector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import cz.siret.knn.DescDistanceComparator;
import cz.siret.knn.KnnMetrics;
import cz.siret.knn.Logger;
import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.SiretConfig;
import cz.siret.knn.SparkMetricsRegistrator;
import cz.siret.knn.SparkUtils;
import cz.siret.knn.metric.IMetric;
import cz.siret.knn.metric.MetricProvider;
import cz.siret.knn.model.*;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Pivot based (Voronoi) approximate kNN join
 * 
 * @author pcech
 *
 */
public class KNNJoinApprox {

	public enum Method {
		EXACT, EXACT_BOUNDS, APPROX, APPROX_BOUNDS, EPSILON_APPROX
	}

	/**
	 * Main execution of the whole job. Must be serializable because Spark requires it.
	 */
	@SuppressWarnings("serial")
	public static class Job implements Serializable {

		private final int SEED = 937;

		private final String databasePath;
		private final String queriesPath;

		private final int k;
		private final int minNumberOfPartitions;
		private final int numberOfReducers;

		private final boolean useCutRegions;
		private final int pivotCount;
		private final int maxRecDepth;
		private final int maxNumberOfBuckets;
		private final int staticPivotCount;
		private final float epsilonParam;
		private final float epsilonMultiplierParam;
		private Method method;

		public Job(String[] args) {

			databasePath = args[0];
			queriesPath = args[1];

			// parsing parameters
			SparkConf conf = new SparkConf();
			numberOfReducers = conf.getInt(SiretConfig.REDUCERS_COUNT_STR, 10);
			minNumberOfPartitions = conf.getInt(SiretConfig.MIN_PARITTIONS_COUNT_STR, 10);
			k = conf.getInt(SiretConfig.K_STR, 10);

			pivotCount = conf.getInt(SiretConfig.PIVOT_COUNT_STR, 1000);
			maxRecDepth = conf.getInt(SiretConfig.MAX_SPLIT_DEPTH_STR, 1);
			maxNumberOfBuckets = (int) (pivotCount * conf.getDouble(SiretConfig.FILTER_STR, 0.1));
			staticPivotCount = conf.getInt(SiretConfig.STATIC_PIVOT_COUNT_STR, 10);
			useCutRegions = staticPivotCount > 0;
			epsilonParam = (float) conf.getDouble(SiretConfig.EPSILON_STR, 1);
			epsilonMultiplierParam = (float) conf.getDouble(SiretConfig.EPSILON_MULTIPLIER_STR, 1);
			try {
				method = Method.valueOf(conf.get(SiretConfig.PIVOT_METHOD_STR).toUpperCase());
			} catch (Exception e) {
				System.out.println("Unknown method type, setting to EXACT");
				method = Method.EXACT;
			}
		}

		public void run(String outputPath) throws Exception {

			System.out.println("Starting Spark with parameters:");
			System.out.println("Reducers: " + numberOfReducers + ", MinPartitions: " + minNumberOfPartitions + ", PivotCount: " + pivotCount + ", StaticPivotCount: "
					+ staticPivotCount + ", MaxSplitDepth: " + maxRecDepth + ", BucketsToVisit: " + maxNumberOfBuckets + ", K: " + k + ", Method: " + method
					+ ", Epsilon: " + epsilonParam + ", EpsilonMultiplier: " + epsilonMultiplierParam);

			SparkConf conf = new SparkConf().setAppName("Pivot kNN join").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, Pair.class, FeatureWithOnePartition.class,
							FeatureWithMultiplePartitions.class, ObjectWithDistance.class, VoronoiStatistics.class, PartitionStatistics.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			KnnMetrics knnMetrics = SparkMetricsRegistrator.register(jsc.sc());
			System.out.println("Startup ready. Serializer: " + jsc.getConf().get("spark.serializer"));

			// delete output directory if exists
			new SparkUtils().deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			LongAccumulator distanceComputations = jsc.sc().longAccumulator();
			LongAccumulator databaseReplications = jsc.sc().longAccumulator();

			JavaRDD<NNResult> kNNResult = computeKnn(jsc, k, method, pivotCount, maxRecDepth, numberOfReducers, maxNumberOfBuckets, distanceComputations,
					databaseReplications);

			if (kNNResult != null) {
				kNNResult.saveAsTextFile(outputPath);
			}
			System.out.println("The kNN job has finished.");
			System.out.println(knnMetrics.produceOutput());
			System.out.println("Distance computations: " + distanceComputations.value());
			System.out.println("Database replications: " + databaseReplications.value());

			jsc.close();
		}

		/**
		 * Computes k nearest neighbors.
		 */
		public JavaRDD<NNResult> computeKnn(JavaSparkContext jsc, final int k, Method method, int pivotCount, int maxRecDepth, int numberOfReducers,
				int maxNumberOfBuckets, LongAccumulator distanceComputations, LongAccumulator databaseReplications) throws Exception {

			Logger logger = new Logger(jsc.getConf().get("spark.submit.deployMode").toLowerCase().equals("client")); // logging only for the client mode
			SparkUtils sparkUtils = new SparkUtils();

			JavaRDD<Feature> databaseFeatures = sparkUtils.readInputPath(jsc, databasePath, minNumberOfPartitions).cache();
			JavaRDD<Feature> queriesFeatures = sparkUtils.readInputPath(jsc, queriesPath, minNumberOfPartitions);

			// pivots
			List<Feature> pivots = getPivots(databaseFeatures, pivotCount);
			Broadcast<List<Feature>> broadcastedPivots = jsc.broadcast(pivots);
			int[] staticPivotIds = getStaticPivotIds(pivots, staticPivotCount, useCutRegions);
			logger.logStaticPivots(staticPivotIds);
			Broadcast<int[]> broadcastedStaticPivotIds = jsc.broadcast(staticPivotIds);
			System.out.println("Pivot selection done.");
			//

			// data split into Voronoi cells
			JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures = getPartitions(databaseFeatures, queriesFeatures, broadcastedPivots,
					broadcastedStaticPivotIds, maxRecDepth, method).persist(StorageLevel.MEMORY_AND_DISK_SER()); // better to save this than compute distances again
			VoronoiStatistics voronoiStatistics = getPartitionStatistics(partitionedFeatures, k, pivotCount, staticPivotCount);
			Broadcast<VoronoiStatistics> broadcastedVoronoiStats = jsc.broadcast(voronoiStatistics);
			logger.logVoronoiStats(voronoiStatistics);

			System.out.println("Feature partitioning done.");
			System.out.println("Voronoi statistics computed.");
			//

			// grouping
			int[] groups = getGroups(voronoiStatistics, pivots, numberOfReducers);
			Broadcast<int[]> broadcastedGroups = jsc.broadcast(groups);
			logger.logGroups(groups, numberOfReducers);

			System.out.println("Grouping done.");
			//

			// kNN computation
			JavaRDD<NNResult> kNNResult = null;
			BoundsCalculator boundsCalculator = new BoundsCalculator();
			switch (method) {
			case APPROX:
				kNNResult = new KNNApproxCalculator(distanceComputations, databaseReplications, maxNumberOfBuckets).computeKNN(partitionedFeatures, k,
						broadcastedPivots, broadcastedVoronoiStats, broadcastedGroups, numberOfReducers, 1, 1);
				break;
			case EXACT_BOUNDS: // set parameters to exact search and do the same computation which is done for the approximate bounds
				maxRecDepth = pivotCount;
				maxNumberOfBuckets = pivotCount;
			case APPROX_BOUNDS:
				// just computes statistics for now
				System.out.println("Computing upper bounds..");
				List<List<Integer>> reverseNearestPivots = boundsCalculator.getReverseNearestPivotsForAllPivots(pivots, maxRecDepth, MetricProvider.getMetric());
				Broadcast<List<List<Integer>>> broadcastedReverseNearestPivots = jsc.broadcast(reverseNearestPivots);
				System.out.println("Preparations have finished.");

				JavaPairRDD<FeaturesKey, Float> upperBoundsForAllQueries = partitionedFeatures.values().filter(new Function<IFeatureWithPartition, Boolean>() {
					public Boolean call(IFeatureWithPartition featureWithPartition) throws Exception {
						return !featureWithPartition.isDatabase();
					}
				}).mapToPair(boundsCalculator.getUpperBoundsForAllQueries(broadcastedGroups, numberOfReducers, broadcastedPivots, broadcastedVoronoiStats,
						broadcastedReverseNearestPivots, k, maxNumberOfBuckets)).cache();

				logger.logUpperBoundsForAllQueries(upperBoundsForAllQueries.collectAsMap(), k, maxRecDepth, maxNumberOfBuckets / (float) pivotCount, pivotCount,
						numberOfReducers);

				kNNResult = upperBoundsForAllQueries.map(new Function<Tuple2<FeaturesKey, Float>, NNResult>() {
					public NNResult call(Tuple2<FeaturesKey, Float> queryWithUpperBounds) throws Exception {

						NNResult result = new NNResult();
						for (int i = 0; i < k - 1; i++) {
							result.dbDistances.add(-1f);
						}
						result.dbDistances.add(queryWithUpperBounds._2);
						return result;
					}
				});
				System.out.println("Done.");
				break;
			case EPSILON_APPROX:
			case EXACT:
				IMetric metric = MetricProvider.getMetric();
				float epsilon = method == Method.EXACT ? 1 : epsilonParam;
				float epsilonMultiplier = method == Method.EXACT ? 1 : epsilonMultiplierParam;
				List<List<ObjectWithDistance>> lbOfPartitionSToGroups = boundsCalculator.getLBPartitionOfSToGroups(groups, numberOfReducers, pivots, staticPivotIds,
						voronoiStatistics, k, metric, epsilon * epsilonMultiplier * epsilonMultiplier);
				logger.logLowerBounds(lbOfPartitionSToGroups);
				Broadcast<List<List<ObjectWithDistance>>> broadcastedLowerBounds = jsc.broadcast(lbOfPartitionSToGroups);
				kNNResult = new KNNExactCalculator(distanceComputations, databaseReplications, broadcastedLowerBounds).computeKNN(partitionedFeatures, k,
						broadcastedPivots, broadcastedVoronoiStats, broadcastedGroups, numberOfReducers, epsilon, epsilonMultiplier);
				break;
			default:
				throw new Exception("Unknown or unimplemented kNN join computation method " + method);
			}
			//

			return kNNResult;
		}

		private List<Feature> getPivots(JavaRDD<Feature> objects, int pivotCount) {

			return objects.takeSample(false, pivotCount, SEED);
		}

		private int[] getStaticPivotIds(List<Feature> pivots, int staticPivotCount, boolean useCutRegions) throws Exception {

			if (!useCutRegions) {
				return new int[0];
			}

			IMetric metric = MetricProvider.getMetric();

			int[] output = new int[staticPivotCount];
			List<Integer> pivotIds = new ArrayList<>();
			for (int i = 0; i < pivots.size(); i++) {
				pivotIds.add(i);
			}

			for (int i = 0; i < staticPivotCount; i++) {

				float maxDist = -Float.MAX_VALUE;
				int pivotId = -1;
				for (int j = 0; j < pivotIds.size(); j++) {

					int nextPivotId = pivotIds.get(j);
					float dist = 0;
					for (int k = 0; k < i; k++) {
						dist += metric.dist(pivots.get(output[k]), pivots.get(nextPivotId));
					}
					if (dist > maxDist) { // a pivot which is the most distant one from selected others is taken
						maxDist = dist;
						pivotId = nextPivotId;
					}
				}

				output[i] = pivotId;
				pivotIds.remove((Object) pivotId);
			}

			Arrays.sort(output);
			return output;
		}

		// not needed now
		// private float[][] computePivotDistances(JavaSparkContext jsc, final Broadcast<List<FeatureWritable>> broadcastedPivots) {
		//
		// // should preserve order
		// return jsc.parallelize(broadcastedPivots.getValue()).map(new Function<FeatureWritable, float[]>() {
		// public float[] call(FeatureWritable pivot) throws Exception {
		//
		// List<FeatureWritable> pivotsLocal = broadcastedPivots.getValue();
		// float[] distancesToOtherPivots = new float[pivotsLocal.size()];
		// IMetric metric = new L2MetricSiret();
		// for (int i = 0; i < distancesToOtherPivots.length; i++) {
		// distancesToOtherPivots[i] = metric.dist(pivot, pivotsLocal.get(i));
		// }
		// return distancesToOtherPivots;
		// }
		// }).collect().toArray(new float[0][]);
		// }

		private JavaPairRDD<Integer, IFeatureWithPartition> getPartitions(JavaRDD<Feature> datasetFile, JavaRDD<Feature> queriesFile,
				Broadcast<List<Feature>> pivots, Broadcast<int[]> staticPivotIds, int numberOfNearestPivots, Method method) {

			return datasetFile.mapToPair(getPartitionFunction(pivots, staticPivotIds, true, numberOfNearestPivots, method))
					.union(queriesFile.mapToPair(getPartitionFunction(pivots, staticPivotIds, false, numberOfNearestPivots, method)));
		}

		/**
		 * Partitions both database and query objects to Voronoi cells according to preselected pivots
		 */
		private PairFunction<Feature, Integer, IFeatureWithPartition> getPartitionFunction(final Broadcast<List<Feature>> pivots,
				final Broadcast<int[]> staticPivotIds, final boolean isDatabase, final int numberOfNearestPivots, final Method method) {

			return new PairFunction<Feature, Integer, IFeatureWithPartition>() {
				public Tuple2<Integer, IFeatureWithPartition> call(Feature feature) throws Exception {

					List<Feature> pivotsLocal = pivots.value();
					int[] staticPivotIdsLocal = staticPivotIds.value();
					if (pivotsLocal.size() < numberOfNearestPivots) {
						throw new Exception("Wanted number of nearest pivots " + numberOfNearestPivots + " is too big");
					}

					final IMetric metric = MetricProvider.getMetric();
					List<ObjectWithDistance> distancesToPivots = new ArrayList<>(pivotsLocal.size());
					float[] distToStaticPivots = new float[staticPivotIdsLocal.length];

					int i = 0;
					for (int j = 0; j < pivotsLocal.size(); j++) {
						float dist = metric.dist(feature, pivotsLocal.get(j));
						distancesToPivots.add(new ObjectWithDistance(j, dist));

						if (i < distToStaticPivots.length && staticPivotIdsLocal[i] == j) {
							distToStaticPivots[i++] = dist;
						}
					}

					Collections.sort(distancesToPivots);

					if (method == Method.APPROX) {
						int[] nearestPivots = new int[numberOfNearestPivots];
						for (i = 0; i < numberOfNearestPivots; i++) {
							nearestPivots[i] = distancesToPivots.get(i).getObjectId();
						}

						return new Tuple2<Integer, IFeatureWithPartition>(distancesToPivots.get(0).getObjectId(),
								new FeatureWithMultiplePartitions(feature, nearestPivots, distancesToPivots.get(0).getDistance(), isDatabase, distToStaticPivots));
					} else {
						return new Tuple2<Integer, IFeatureWithPartition>(distancesToPivots.get(0).getObjectId(), new FeatureWithOnePartition(feature,
								distancesToPivots.get(0).getObjectId(), distancesToPivots.get(0).getDistance(), isDatabase, distToStaticPivots));
					}
				}
			};
		}

		/**
		 * Computes statistics for each Voronoi cell for both database and query objects
		 * 
		 * @param pivotCount
		 */
		private VoronoiStatistics getPartitionStatistics(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures, final int k, int pivotCount,
				final int staticPivotCount) {

			// TODO - maybe be optimized using reduceByKey instead of groupByKey - but will it be better?
			List<Tuple3<Integer, PartitionStatistics, PartitionStatistics>> statistics = partitionedFeatures.groupByKey()
					.map(new Function<Tuple2<Integer, Iterable<IFeatureWithPartition>>, Tuple3<Integer, PartitionStatistics, PartitionStatistics>>() {

						class Statistics {
							public float maxDist = -Float.MAX_VALUE;
							public float minDist = Float.MAX_VALUE;
							public int numberOfObjects = 0;
							public long sizeOfObjects = 0;
							public PriorityQueue<Float> distancesToNearestObjects = new PriorityQueue<>(k, new DescDistanceComparator());
							public CutRegion cutRegion = null;
						}

						public Tuple3<Integer, PartitionStatistics, PartitionStatistics> call(Tuple2<Integer, Iterable<IFeatureWithPartition>> partition) throws Exception {

							Statistics dbStats = new Statistics(), queryStats = new Statistics();
							dbStats.cutRegion = new CutRegion(staticPivotCount);

							for (IFeatureWithPartition feature : partition._2) {
								if (feature.isDatabase()) {
									updateStats(feature, dbStats, true);
								} else {
									updateStats(feature, queryStats, false);
								}
							}

							// there might be less objects than K
							float[] distances = new float[dbStats.distancesToNearestObjects.size()];
							for (int i = distances.length - 1; i >= 0; i--) {
								distances[i] = dbStats.distancesToNearestObjects.poll();
							}

							return new Tuple3<>(partition._1,
									new PartitionStatistics(dbStats.minDist, dbStats.maxDist, dbStats.numberOfObjects, dbStats.sizeOfObjects, distances, dbStats.cutRegion),
									new PartitionStatistics(queryStats.minDist, queryStats.maxDist, queryStats.numberOfObjects, queryStats.sizeOfObjects));
						}

						private void updateStats(IFeatureWithPartition feature, Statistics statistics, boolean isDatabase) {
							if (feature.getDistanceToPivot() > statistics.maxDist) {
								statistics.maxDist = feature.getDistanceToPivot();
							}
							if (feature.getDistanceToPivot() < statistics.minDist) {
								statistics.minDist = feature.getDistanceToPivot();
							}
							statistics.numberOfObjects++;
							statistics.sizeOfObjects += feature.getFeature().getValues().length;// approximation

							if (isDatabase) {
								statistics.cutRegion.addFeature(feature);

								if (statistics.distancesToNearestObjects.size() < k || statistics.distancesToNearestObjects.peek() > feature.getDistanceToPivot()) {
									statistics.distancesToNearestObjects.add(feature.getDistanceToPivot());
									if (statistics.distancesToNearestObjects.size() > k) {
										statistics.distancesToNearestObjects.remove();
									}
								}
							}
						}

					}).collect();

			PartitionStatistics[] databaseStatistics = new PartitionStatistics[pivotCount];
			PartitionStatistics[] queryStatistics = new PartitionStatistics[pivotCount];
			for (Tuple3<Integer, PartitionStatistics, PartitionStatistics> tuple : statistics) {
				databaseStatistics[tuple._1()] = tuple._2();
				queryStatistics[tuple._1()] = tuple._3();
			}
			// theoretically there might be some partitions with no objects (if pivots are overlapping)
			for (int i = 0; i < pivotCount; i++) {
				if (databaseStatistics[i] == null) {
					databaseStatistics[i] = new PartitionStatistics();
				}
				if (queryStatistics[i] == null) {
					queryStatistics[i] = new PartitionStatistics();
				}
			}

			return new VoronoiStatistics(databaseStatistics, queryStatistics);
		}

		/**
		 * Returns an array of pivot count length where each bin says the group ID for a specific pivot
		 * 
		 * @param numberOfGroups
		 */
		private int[] getGroups(VoronoiStatistics voronoiStats, List<Feature> pivots, int numberOfGroups) throws Exception {

			PartitionStatistics[] queryStatistics = voronoiStats.getQueryStatistics();
			List<Long> sizeOfObjects = new ArrayList<>();
			List<Integer> numberOfObjects = new ArrayList<>();
			for (PartitionStatistics stats : queryStatistics) {
				sizeOfObjects.add(stats.getSizeOfObjects());
				numberOfObjects.add(stats.getNumberOfObjects());
			}

			// TODO maybe pass pivot distance matrix directly
			GeometricGroupingBySize grouping = new GeometricGroupingBySize(MetricProvider.getMetric(), new ArrayList<Object>(pivots), sizeOfObjects, numberOfObjects,
					numberOfGroups);
			Vector<Integer>[] groups = grouping.doGouping();

			int[] result = new int[pivots.size()];
			int groupId = 0;
			for (Vector<Integer> group : groups) {
				for (Integer pivotId : group) {
					result[pivotId] = groupId;
				}
				groupId++;
			}
			return result;
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
