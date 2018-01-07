package cz.siret.knn.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import cz.siret.knn.model.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import cz.siret.knn.KnnMetrics;
import cz.siret.knn.Logger;
import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.SiretConfig;
import cz.siret.knn.SparkMetricsRegistrator;
import cz.siret.knn.SparkUtils;
import cz.siret.knn.naive.NaiveKnnCalculator;
import scala.Tuple2;

/**
 * LSH approximate kNN join
 * 
 * @author pcech
 *
 */
public class KNNJoinApprox {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final int SEED = 100;

		private final String databasePath;
		private final String queryPath;

		private final int k;
		private final int minNumberOfPartitions;
		private final int reducers;

		private final int numHashTables;
		private final int hashFunctions;
		private final double w;
		private final int hashFuncMultiplier;

		public Job(String[] args) {

			databasePath = args[0];
			queryPath = args[1];

			// parsing parameters
			SparkConf conf = new SparkConf();
			reducers = conf.getInt(SiretConfig.REDUCERS_COUNT_STR, 10);
			minNumberOfPartitions = conf.getInt(SiretConfig.MIN_PARITTIONS_COUNT_STR, 10);
			k = conf.getInt(SiretConfig.K_STR, 10);

			numHashTables = conf.getInt(SiretConfig.HASH_TABLES_STR, 2);
			hashFunctions = conf.getInt(SiretConfig.HASH_FUNCTIONS_STR, 20);
			w = conf.getDouble(SiretConfig.W_STR, 100);
			hashFuncMultiplier = conf.getInt(SiretConfig.HASH_FUNC_MULTIPLIER_STR, 2);
		}

		public void run(String outputPath) throws Exception {

			SparkConf conf = new SparkConf().setAppName("LSH kNN join").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class, HashPartition.class, HashTable.class,
							PartialDimensionStats.class, DimensionStats.class, ObjectWithDistance.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);
			KnnMetrics knnMetrics = SparkMetricsRegistrator.register(jsc.sc());
			Logger logger = new Logger(jsc.getConf().get("spark.submit.deployMode").toLowerCase().equals("client")); // logging only for the client mode
			SparkUtils sparkUtils = new SparkUtils();
			sparkUtils.deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			JavaRDD<Feature> databaseFeatures = sparkUtils.readInputPath(jsc, databasePath, minNumberOfPartitions).cache();
			JavaRDD<Feature> queriesFeatures = sparkUtils.readInputPath(jsc, queryPath, minNumberOfPartitions);

			int dimension = getDimension(databaseFeatures);
			printParameters(jsc.getConf().get("spark.serializer"), dimension);

			// normal transformation
			DimensionStats[] dimensionStats = getDimensionStats(databaseFeatures, dimension);
			Broadcast<DimensionStats[]> broadcastedDimensionStats = jsc.broadcast(dimensionStats);
			logger.logDimensionStats(dimensionStats);
			System.out.println("Dimension statistics computed.");
			//

			// hash tables
			HashTable[] hashTables;
			if (hashFuncMultiplier == 1) {
				hashTables = getSimpleHashTables(dimension, numHashTables, hashFunctions, w);
			} else {
				Broadcast<HashFunction[]> broadcastedHashFunctions = jsc.broadcast(getHashFunctions(dimension, numHashTables, hashFunctions, w, hashFuncMultiplier));
				hashTables = getHashTables(databaseFeatures, broadcastedDimensionStats, broadcastedHashFunctions, dimension, numHashTables, hashFunctions);
				broadcastedHashFunctions.destroy();
			}
			Broadcast<HashTable[]> broadcastedHashTables = jsc.broadcast(hashTables);
			logger.logHashTables(hashTables);
			System.out.println("Hash tables prepared.");
			//

			// hashing
			JavaPairRDD<Iterable<Feature>, Iterable<Feature>> partitionsForKnnComputation = getPartitionsForKnnComputation(queriesFeatures,
					databaseFeatures, broadcastedHashTables, broadcastedDimensionStats, reducers);

			// kNN computation
			JavaRDD<NNResult> kNNResult = new NaiveKnnCalculator().calculateKnn(partitionsForKnnComputation, k);
			//

			kNNResult.saveAsTextFile(outputPath);
			System.out.println("LSH kNN join done!");
			System.out.println(knnMetrics.produceOutput());

			jsc.close();
		}

		private int getDimension(JavaRDD<Feature> queriesFeatures) {

			int[] histogramKeys = queriesFeatures.take(1).get(0).getKeys();
			return histogramKeys[histogramKeys.length - 1] + 1;
		}

		private void printParameters(String serializer, int dimension) {
			System.out.println("Starting computing LSH kNN join. Parameters: ");
			System.out.println("Startup ready. Serializer: " + serializer);
			System.out.println("K: " + k + ", Reducers: " + reducers + ", Dimensions: " + dimension + ", HashTables: " + numHashTables + ", HashFunctions: "
					+ hashFunctions + ", W: " + w + ", HashFuncMultiplier: " + hashFuncMultiplier);
		}

		private DimensionStats[] getDimensionStats(JavaRDD<Feature> features, final int dimensions) {

			return getExpectedValAndStd(features.map(new Function<Feature, float[]>() {

				public float[] call(Feature feature) throws Exception {
					float[] output = new float[dimensions];
					for (Feature.DimValue dv : feature.iterate(dimensions)) {
						output[dv.dim] = dv.value;
					}
					return output;
				}
			}), dimensions);
		}

		private DimensionStats[] getExpectedValAndStd(JavaRDD<float[]> valuesRdd, final int dimensions) {

			List<Tuple2<Integer, PartialDimensionStats>> partialStats = valuesRdd
					.mapPartitionsToPair(new PairFlatMapFunction<Iterator<float[]>, Integer, PartialDimensionStats>() {
						public Iterator<Tuple2<Integer, PartialDimensionStats>> call(Iterator<float[]> values) throws Exception {

							final PartialDimensionStats[] output = new PartialDimensionStats[dimensions];
							for (int i = 0; i < output.length; i++) {
								output[i] = new PartialDimensionStats();
							}

							while (values.hasNext()) {
								float[] value = values.next();

								for (int i = 0; i < value.length; i++) {
									output[i].sum += value[i];
									output[i].sumSquares += value[i] * value[i];
									output[i].count++;
								}
							}

							return new Iterator<Tuple2<Integer, PartialDimensionStats>>() {

								int index = 0;

								public boolean hasNext() {
									return index < output.length;
								}

								public Tuple2<Integer, PartialDimensionStats> next() {
									return new Tuple2<>(index, output[index++]);
								}
							};
						};
					}).reduceByKey(new Function2<PartialDimensionStats, PartialDimensionStats, PartialDimensionStats>() {
						public PartialDimensionStats call(PartialDimensionStats stats1, PartialDimensionStats stats2) throws Exception {
							PartialDimensionStats result = new PartialDimensionStats();
							result.sum = stats1.sum + stats2.sum;
							result.sumSquares = stats1.sumSquares + stats2.sumSquares;
							result.count = stats1.count + stats2.count;
							return result;
						}
					}).collect();

			DimensionStats[] output = new DimensionStats[dimensions];
			for (Tuple2<Integer, PartialDimensionStats> tuple : partialStats) {

				PartialDimensionStats stats = tuple._2;
				double expectedValue = stats.sum / stats.count;
				double standardDeviation = Math.sqrt(stats.sumSquares / stats.count - expectedValue * expectedValue);
				output[tuple._1] = new DimensionStats(expectedValue, standardDeviation);
			}
			return output;
		}

		private HashTable[] getSimpleHashTables(int numDimensions, int numHashTables, int numHashFunctions, double w) {

			Random r = new Random(SEED);
			final HashTable[] tables = new HashTable[numHashTables];

			for (int i = 0; i < numHashTables; i++) {

				HashFunction[] hashFuncs = new HashFunction[numHashFunctions];
				for (int j = 0; j < numHashFunctions; j++) {
					hashFuncs[j] = new HashFunction(numDimensions, w, r);
				}
				tables[i] = new HashTable(hashFuncs);
			}
			return tables;
		}

		private HashFunction[] getHashFunctions(int numDimensions, int numHashTables, int numHashFunctions, double w, int generatorMultiplier) {
			final int hashingFunctionsToTest = numHashFunctions * numHashTables * generatorMultiplier;
			Random r = new Random(SEED);

			// generate more hashing functions
			HashFunction[] hashFunctions = new HashFunction[hashingFunctionsToTest];
			for (int i = 0; i < hashingFunctionsToTest; i++) {
				hashFunctions[i] = new HashFunction(numDimensions, w, r);
			}
			return hashFunctions;
		}

		private HashTable[] getHashTables(JavaRDD<Feature> databaseFeatures, final Broadcast<DimensionStats[]> dimensionStats,
				final Broadcast<HashFunction[]> hashFunctions, int dimensions, int numHashTables, int numHashFunctions) {

			HashFunction[] hashFunctionsLocal = hashFunctions.value();

			// select only hash functions with the highest deviation on a data sample
			DimensionStats[] stats = getExpectedValAndStd(databaseFeatures.sample(false, 0.5, SEED).map(new Function<Feature, float[]>() {

				private HashFunction[] hashFunctionsLocal = hashFunctions.value();
				private DimensionStats[] dimensionStatsLocal = dimensionStats.value();

				public float[] call(Feature feature) throws Exception {

					float[] output = new float[hashFunctionsLocal.length];
					for (int i = 0; i < hashFunctionsLocal.length; i++) {
						output[i] = hashFunctionsLocal[i].evaluate(feature, dimensionStatsLocal);
					}
					return output;
				}
			}), hashFunctionsLocal.length);

			List<Tuple2<Integer, Double>> statsWithIndex = new ArrayList<>(stats.length);
			for (int i = 0; i < stats.length; i++) {
				statsWithIndex.add(new Tuple2<>(i, stats[i].getStandardDeviation()));
			}
			Collections.sort(statsWithIndex, new Comparator<Tuple2<Integer, Double>>() {
				public int compare(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) {
					return -t1._2.compareTo(t2._2); // descending ordering
				}
			});

			final HashTable[] tables = new HashTable[numHashTables];
			int index = 0;
			for (int i = 0; i < numHashTables; i++) {

				HashFunction[] hashFuncs = new HashFunction[numHashFunctions];
				for (int j = 0; j < numHashFunctions; j++) {
					hashFuncs[j] = hashFunctionsLocal[statsWithIndex.get(index++)._1];
				}
				tables[i] = new HashTable(hashFuncs);
			}
			return tables;
		}

		private JavaPairRDD<Iterable<Feature>, Iterable<Feature>> getPartitionsForKnnComputation(JavaRDD<Feature> queries,
				JavaRDD<Feature> database, final Broadcast<HashTable[]> hashTables, Broadcast<DimensionStats[]> dimensionStats, int reducers) {

			// TODO balancing? - is now better in Spark, maybe not needed
			return JavaPairRDD.fromJavaRDD(queries.flatMapToPair(getPartitionFunction(hashTables, dimensionStats))
					.cogroup(database.flatMapToPair(getPartitionFunction(hashTables, dimensionStats)), reducers).values());
		}

		private PairFlatMapFunction<Feature, HashPartition, Feature> getPartitionFunction(final Broadcast<HashTable[]> hashTables,
				final Broadcast<DimensionStats[]> dimensionStats) {

			return new PairFlatMapFunction<Feature, HashPartition, Feature>() {
				public Iterator<Tuple2<HashPartition, Feature>> call(Feature feature) throws Exception {

					HashTable[] localHashTables = hashTables.value();
					DimensionStats[] dimensionStatsLocal = dimensionStats.value();
					List<Tuple2<HashPartition, Feature>> output = new ArrayList<>(localHashTables.length);

					for (int i = 0; i < localHashTables.length; i++) {
						output.add(new Tuple2<>(new HashPartition(i, localHashTables[i].evaluate(feature, dimensionStatsLocal)), feature));
					}
					return output.iterator();
				}
			};
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
