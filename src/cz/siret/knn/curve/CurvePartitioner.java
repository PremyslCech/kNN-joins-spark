package cz.siret.knn.curve;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;

@SuppressWarnings("serial")
public class CurvePartitioner implements Serializable {

	/**
	 * Gets partition points for each shift in a sorted order. There are separate partitions for database points and query points. Each query point goes to one
	 * partition, each database point could go to multiple partitions (replication). Query partitions does not overlap, database partitions does overlap to
	 * statistically ensure k neighbors for each query.
	 * 
	 * @return
	 */
	public Partitions getPartitions(JavaPairRDD<Integer, ICurveOrder> queriesWithCurve, JavaPairRDD<Integer, ICurveOrder> databaseWithCurve,
			double dbProbability, double queryProbability, int k, int numberOfPartitions, int dimension, int shifts) {

		Map<Integer, Iterable<ICurveOrder>> sampledQueries = queriesWithCurve.sample(false, queryProbability).groupByKey().collectAsMap();
		Map<Integer, Iterable<ICurveOrder>> sampledDb = databaseWithCurve.sample(false, dbProbability).groupByKey().collectAsMap();

		List<List<PartitionBoundaries>> queryPartitions = new ArrayList<>(shifts);
		List<List<PartitionBoundaries>> dbPartitions = new ArrayList<>(shifts);
		for (int i = 0; i < shifts; i++) {
			queryPartitions.add(new ArrayList<PartitionBoundaries>());
			dbPartitions.add(new ArrayList<PartitionBoundaries>());
		}
		int newKnn = (int) Math.ceil(k * dbProbability);

		for (Entry<Integer, Iterable<ICurveOrder>> pair : sampledQueries.entrySet()) {

			int shiftId = pair.getKey();
			
			// loading and parsing input
			List<BigInteger> orderedQueries = new ArrayList<>();
			for (ICurveOrder curveWithFeature : pair.getValue()) {
				orderedQueries.add(curveWithFeature.getCurveOrder());
			}
			Collections.sort(orderedQueries);
			int queriesOriginalCount = (int) (orderedQueries.size() / queryProbability);

			List<BigInteger> dbObjects = new ArrayList<>();
			for (ICurveOrder curveWithFeature : sampledDb.get(shiftId)) {
				dbObjects.add(curveWithFeature.getCurveOrder());
			}
			Collections.sort(dbObjects);

			List<PartitionBoundaries> queryPartitionsForShift = queryPartitions.get(shiftId);
			List<PartitionBoundaries> dbPartitionsForShift = dbPartitions.get(shiftId);
			//

			BigInteger q_start = ZorderHelper.minValue();
			for (int i = 1; i <= numberOfPartitions; i++) {

				int estRank = getEstimatorIndex(i, queriesOriginalCount, queryProbability, numberOfPartitions);
				if (estRank - 1 >= orderedQueries.size()) {
					estRank = orderedQueries.size();
				}

				BigInteger q_end = i == numberOfPartitions ? ZorderHelper.maxValue(dimension) : orderedQueries.get(estRank - 1);
				queryPartitionsForShift.add(new PartitionBoundaries(q_start, q_end));

				int low;
				if (i == 1) {
					low = 0;
				} else {
					low = Collections.binarySearch(dbObjects, q_start);
					if (low < 0) // binary search returns: -(insert position) - 1
						low = -low - 1;
					if ((low - newKnn) < 0)
						low = 0;
					else
						low -= newKnn;
				}

				BigInteger s_start = i == 1 ? ZorderHelper.minValue() : dbObjects.get(low);

				int high;
				if (i == numberOfPartitions) {
					high = dbObjects.size() - 1;
				} else {
					high = Collections.binarySearch(dbObjects, q_end);
					if (high < 0)
						high = -high - 1;
					if ((high + newKnn) > dbObjects.size() - 1)
						high = dbObjects.size() - 1;
					else
						high += newKnn;
				}

				BigInteger s_end = i == numberOfPartitions ? ZorderHelper.maxValue(dimension) : dbObjects.get(high);
				dbPartitionsForShift.add(new PartitionBoundaries(s_start, s_end));

				q_start = q_end.add(BigInteger.ONE);
			}
		}

		return new Partitions(queryPartitions, dbPartitions);
	}

	/**
	 * Calculates the index of the estimator for i-th q-quantiles in a given sampled data set (size).
	 */
	private int getEstimatorIndex(int i, long size, double sampleRate, int numOfPartition) {
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));

		int val1 = (int) Math.floor(orgRank * sampleRate);
		int val2 = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = (int) Math.abs(est1 - orgRank);
		int dist2 = (int) Math.abs(est2 - orgRank);

		return dist1 < dist2 ? val1 : val2;
	}
	
	public static void main(String[] args) {
		
		CurvePartitioner curvePartitioner = new CurvePartitioner();
		curvePartitioner.getEstimatorIndex(1, 100000, 0.001, 20);
		
	}

}
