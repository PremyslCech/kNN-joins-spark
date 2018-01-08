package cz.siret.knn.eval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import cz.siret.knn.model.FeaturesKey;
import cz.siret.knn.model.NNResult;
import scala.Tuple2;

/**
 * Evaluates an approximation precision and distance differences between exact and approximation results.
 * 
 * @author pcech
 *
 */
public class PrecisionEvaluator {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		private final String basePath;
		private final String operator;
		private final String comparePath;

		public Job(String[] args) {

			basePath = args[0];
			operator = args[1];
			comparePath = args[2];
		}

		public void run() throws Exception {

			SparkConf conf = new SparkConf().setAppName("Precision evaluation").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { FeaturesKey.class, PrecisionStats.class, NNResult.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);

			JavaRDD<String> baseFile = jsc.textFile(basePath).cache();
			JavaRDD<String> compareFile = jsc.textFile(comparePath).cache();

			List<Tuple2<Integer, PrecisionStats>> sortedStats = getStatsForKValues(baseFile.mapToPair(getParseKnnResultsFunc()),
					compareFile.mapToPair(getParseKnnResultsFunc()), operator).sortByKey().collect();

			System.out.println("K\tDistance\tPrecision");
			long totalCount = Math.max(baseFile.count(), compareFile.count());
			for (Tuple2<Integer, PrecisionStats> tuple : sortedStats) {

				long count = tuple._2.getCount();
				int k = tuple._1;
				if (totalCount != count) {
					System.err.println("Queries count " + count + " is not equal to total count " + totalCount + " for K " + k);
				}

				System.out.println(k + "\t" + tuple._2.getOutput(totalCount));
			}

			jsc.close();
		}

		private PairFunction<String, FeaturesKey, NNResult> getParseKnnResultsFunc() {

			return new PairFunction<String, FeaturesKey, NNResult>() {
				public Tuple2<FeaturesKey, NNResult> call(String line) throws Exception {
					NNResult kNnResult = NNResult.parse(line);
					return new Tuple2<FeaturesKey, NNResult>(kNnResult.queryKey, kNnResult);
				}
			};
		}

		private JavaPairRDD<Integer, PrecisionStats> getStatsForKValues(JavaPairRDD<FeaturesKey, NNResult> baseKnnResults,
				JavaPairRDD<FeaturesKey, NNResult> compareKnnResults, final String operator) {

			return baseKnnResults.cogroup(compareKnnResults)
					.flatMapToPair(new PairFlatMapFunction<Tuple2<FeaturesKey, Tuple2<Iterable<NNResult>, Iterable<NNResult>>>, Integer, PrecisionStats>() {

						final float epsilon = Math.ulp(1f); // machine epsilon for float

						public Iterator<Tuple2<Integer, PrecisionStats>> call(Tuple2<FeaturesKey, Tuple2<Iterable<NNResult>, Iterable<NNResult>>> kNNResults)
								throws Exception {

							FeaturesKey queryKey = kNNResults._1;
							NNResult base = getKnnResults(kNNResults._2._1.iterator(), queryKey);
							NNResult compare = getKnnResults(kNNResults._2._2.iterator(), queryKey);

							checkKnnResultsRelations(operator, queryKey, base, compare);

							List<Tuple2<Integer, PrecisionStats>> output = new ArrayList<>();
							if (base != null && compare != null) {

								int maxK = Math.min(base.dbKeys.size(), compare.dbKeys.size());

								for (int k = 0; k < maxK; k++) {

									float distanceDiff = compare.dbDistances.get(k) - base.dbDistances.get(k);
									if (Float.isNaN(distanceDiff) || Float.isInfinite(distanceDiff)) {
										throw new Exception("avgRatio or maxRatio is NaN for " + queryKey);
									}

									float precision = getPrecisionForK(queryKey, base, compare, k);
									float distRatio = getDistanceRatioForK(base, compare, k);
									float effectiveError = getEffectiveErrorForK(base, compare, k);
									float avgEffectiveError = getAverageEffectiveErrorForK(base, compare, k);

									List<ApproxMeasure> approxMeasures = Arrays.asList(new ApproxMeasure("DistanceDiff", distanceDiff), new ApproxMeasure("Precision", precision),
											new ApproxMeasure("DistanceRatio", distRatio), new ApproxMeasure("EffectiveError", effectiveError),
											new ApproxMeasure("AvgEffectiveError", avgEffectiveError));

									output.add(new Tuple2<>(k + 1, new PrecisionStats(approxMeasures, 1)));
								}
							}

							return output.iterator();
						}

						private NNResult getKnnResults(Iterator<NNResult> iterator, FeaturesKey queryKey) throws Exception {
							NNResult result = null;
							if (iterator.hasNext()) {
								result = iterator.next();
							}
							if (iterator.hasNext()) {
								throw new Exception("More results for one query " + queryKey);
							}
							return result;
						}

						private void checkKnnResultsRelations(final String operator, FeaturesKey queryKey, NNResult base, NNResult compare) throws Exception {
							// check the sets relation
							if (base == null && compare == null)
								throw new Exception("No baseInfo nor cmpInfo for " + queryKey + " (weird)");
							else if (operator.equals("le") && compare == null)
								throw new Exception("No cmpInfo for " + queryKey + " when 'le' relation is set");
							else if (operator.equals("ge") && base == null)
								throw new Exception("No baseInfo for " + queryKey + " when 'ge' relation is set");
							else if (operator.equals("eq") && (base == null || compare == null))
								throw new Exception("No baseInfo or cmpInfo for " + queryKey + " when 'eq' relation is set");
							else if (base.equals(compare)) {
								throw new Exception("The baseInfo and cmpInfo equals");
							}
						}

						public float getPrecisionForK(FeaturesKey queryKey, NNResult base, NNResult compare, int k) throws Exception {

							Set<FeaturesKey> referenceKeys = new HashSet<>(), compareKeys = new HashSet<>();
							float common = 0;
							for (int i = 0, j = 0; i <= k && j <= k;) {

								// we consider a neighbor match when either keys are equal or distances are very similar
								if (base.dbKeys.get(i).equals(compare.dbKeys.get(j)) || Math.abs(base.dbDistances.get(i) - compare.dbDistances.get(j)) < epsilon) {
									checkSameDbKeys(referenceKeys, base.dbKeys.get(i), queryKey);
									checkSameDbKeys(compareKeys, compare.dbKeys.get(j), queryKey);
									common++;
									i++;
									j++;
								} else if (base.dbDistances.get(i) < compare.dbDistances.get(j)) {
									checkSameDbKeys(referenceKeys, base.dbKeys.get(i), queryKey);
									i++;
								} else {
									checkSameDbKeys(compareKeys, compare.dbKeys.get(j), queryKey);
									j++;
								}
							}
							return common / (k + 1);
						}

						public float getDistanceRatioForK(NNResult base, NNResult compare, int k) throws Exception {

							float d1 = 0, d2 = 0;
							for (int i = 0; i <= k; i++) {
								d1 += base.dbDistances.get(i);
								d2 += compare.dbDistances.get(i);
							}
							CheckDistances(d1, d2);

							return d2 == 0 ? 1 : d1 / d2;
						}

						public float getEffectiveErrorForK(NNResult base, NNResult compare, int k) throws Exception {

							float compareDist = compare.dbDistances.get(k);
							float baseDist = base.dbDistances.get(k);
							CheckDistances(baseDist, compareDist);

							if (baseDist == 0) { // if both distances == 0 => returns 1, otherwise returns compareDist + 1
								return compareDist + 1;
							}

							return compareDist / baseDist;
						}

						private void CheckDistances(float baseDist, float compareDist) throws Exception {
							final float tolerance = 0.0001f;
							if (baseDist - tolerance > compareDist) {
								throw new Exception("Wierd base kNN. (base: " + baseDist + ", compare: " + compareDist + ")");
							}
						}

						public float getAverageEffectiveErrorForK(NNResult base, NNResult compare, int k) throws Exception {

							float sum = 0;
							for (int i = 0; i <= k; i++) {
								sum += getEffectiveErrorForK(base, compare, i);
							}
							return sum / (k + 1);
						}

						private void checkSameDbKeys(Set<FeaturesKey> usedKeys, FeaturesKey keyToAdd, FeaturesKey queryKey) throws Exception {
							if (!usedKeys.add(keyToAdd)) {
								throw new Exception("Multiple keys " + keyToAdd + " for the query " + queryKey);
							}
						}

					}).reduceByKey(new Function2<PrecisionStats, PrecisionStats, PrecisionStats>() {
						public PrecisionStats call(PrecisionStats stats1, PrecisionStats stats2) throws Exception {
							return stats1.combine(stats2);
						}
					});
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Wrong number of arguments, usage is: <base> <operator> <compare>");
			return;
		}

		Job job = new Job(args);
		job.run();
	}
}
