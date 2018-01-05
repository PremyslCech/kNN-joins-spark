package cz.siret.knn.pivot;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import cz.siret.knn.DescDistanceComparator;
import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.metric.IMetric;
import cz.siret.knn.metric.MetricProvider;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;
import scala.Tuple2;

/**
 * Class for all upper and lower bounds computations in a metric space.
 * 
 * @author pcech
 *
 */
@SuppressWarnings("serial")
public class BoundsCalculator implements Serializable {

	/**
	 * Computes LOWER BOUND for every DATABASE partition for all groups.
	 * 
	 * @return For every database partition outputs sorted list of groups with the lower bound for the group in ascending order.
	 */
	public List<List<ObjectWithDistance>> getLBPartitionOfSToGroups(int[] groupsInList, int numberOfGroups, List<Feature> pivots, int[] staticPivotIds,
			VoronoiStatistics voronoiStatistics, int k, IMetric metric, float epsilon) throws Exception {

		List<List<ObjectWithDistance>> output = new ArrayList<>();
		for (int i = 0; i < pivots.size(); i++) {
			output.add(new ArrayList<ObjectWithDistance>());
		}

		List<List<Integer>> groups = invertGroupList(groupsInList, numberOfGroups);

		for (int i = 0; i < numberOfGroups; i++) {
			List<Integer> group = groups.get(i);
			// space complexity: group.size * numOfPivots
			float[][] distMatrix = computeDistanceMatrix(pivots, metric, group);
			float[] upperBoundForR = getUpperBound(group, distMatrix, voronoiStatistics, k, epsilon);

			// for (int j = 0; j < group.size(); j++) {
			// System.out.print(upperBoundForR[j] + " ");
			// }
			// System.out.println();

			output = initLBOfPartitionS(i, group, distMatrix, upperBoundForR, output, voronoiStatistics, staticPivotIds);
		}

		for (int i = 0; i < output.size(); i++) {
			Collections.sort(output.get(i));
		}
		return output;
	}

	/**
	 * Computes upper bound statistics for all query objects. The upper bound is computed from Voronoi cells in a group that contains the query object plus all
	 * cells that will be replicated there and are the closest FilterRatio cells to the query. Just for testing purposes.
	 * 
	 */
	public PairFunction<IFeatureWithPartition, FeaturesKey, Float> getUpperBoundsForAllQueries(final Broadcast<int[]> groupsInList,
			final int numberOfGroups, final Broadcast<List<Feature>> pivots, final Broadcast<VoronoiStatistics> voronoiStatistics,
			final Broadcast<List<List<Integer>>> reverseNearestPivots, final int k, final int maxNumberOfBuckets) throws Exception {

		return new PairFunction<IFeatureWithPartition, FeaturesKey, Float>() {

			private final List<List<Integer>> groups = invertGroupList(groupsInList.value(), numberOfGroups);
			private final PartitionStatistics[] databaseStatistics = voronoiStatistics.value().getDatabaseStatistics();

			public Tuple2<FeaturesKey, Float> call(IFeatureWithPartition query) throws Exception {

				List<List<Integer>> reverseNearestPivotsLocal = reverseNearestPivots.value();
				int[] groupsInListLocal = groupsInList.value();
				List<Feature> pivotsLocal = pivots.value();
				PriorityQueue<Float> knn = new PriorityQueue<Float>(k, new DescDistanceComparator());

				// upper bound is computed from Voronoi cells in a group that contains the query object plus all cells that will be replicated there
				int queryNearestPivot = query.getNearestPivot();
				HashSet<Integer> pivotsInGroup = new HashSet<>();
				for (int pivotId : groups.get(groupsInListLocal[queryNearestPivot])) {
					for (int pivId : reverseNearestPivotsLocal.get(pivotId)) {
						pivotsInGroup.add(pivId); // distinct pivots
					}
				}

				float ubMaxRecDepth = -Float.MAX_VALUE;
				// distances between the query and all pivots are evaluated and only the "maxNumberOfBuckets" closest cells are considered
				List<ObjectWithDistance> querydistToPivots = computeDistanceToOtherPivots(query, pivotsInGroup, pivotsLocal, maxNumberOfBuckets);
				for (ObjectWithDistance pivotWithDist : querydistToPivots) {

					int pivotId = pivotWithDist.getObjectId();
					float queryPivotDistance = pivotWithDist.getDistance();
					PartitionStatistics databaseStats = databaseStatistics[pivotId];

					// fail safe in case there are not enough database neighbors in Voronoi cells
					float maxDist = queryPivotDistance + databaseStats.getMaxDist();
					if (ubMaxRecDepth < maxDist) { // maximum
						ubMaxRecDepth = maxDist;
					}

					for (float distToNearestNeighbor : databaseStats.getDistancesToNearestObjects()) {

						maxDist = queryPivotDistance + distToNearestNeighbor;
						if (knn.size() < k) {
							knn.add(maxDist);
						} else if (maxDist < knn.peek()) {
							knn.remove();
							knn.add(maxDist);
						} else { // distances to neighbors are sorted
							break;
						}
					}
				}

				// check
				if (knn.size() != k) {
					System.out.println("Not enough neighbors (" + knn.size() + ") for queries in the partition " + queryNearestPivot);
				} else {
					if (ubMaxRecDepth < knn.peek()) { // this should never happen
						throw new Exception("Upper bound of a Vonoroni cells is lower then a distance to the Kth neighbor (really weird)!");
					}
					ubMaxRecDepth = knn.peek();
				}

				return new Tuple2<>(query.getFeature().getKey(), ubMaxRecDepth);
			}
		};
	}

	/**
	 * Computes RNN for all pivot IDs not farther than maxRecDepth
	 * 
	 * @param pivots
	 * @param maxRecDepth
	 * @return
	 * @throws IOException
	 */
	public List<List<Integer>> getReverseNearestPivotsForAllPivots(List<Feature> pivots, int maxRecDepth, IMetric metric) throws IOException {

		List<List<Integer>> output = new ArrayList<>(pivots.size());
		for (int i = 0; i < pivots.size(); i++) {
			output.add(new ArrayList<Integer>());
		}

		PriorityQueue<ObjectWithDistance> nearestPivots = new PriorityQueue<>(maxRecDepth, new Comparator<ObjectWithDistance>() {
			public int compare(ObjectWithDistance o1, ObjectWithDistance o2) {
				return -Float.compare(o1.getDistance(), o2.getDistance()); // descending ordering
			}
		});

		for (int pivotId = 0; pivotId < pivots.size(); pivotId++) {

			nearestPivots.clear();
			for (int i = 0; i < pivots.size(); i++) {

				nearestPivots.add(new ObjectWithDistance(i, metric.dist(pivots.get(pivotId), pivots.get(i))));
				if (nearestPivots.size() > maxRecDepth) {
					nearestPivots.remove();
				}
			}

			for (ObjectWithDistance pivotWithDist : nearestPivots) {
				output.get(pivotWithDist.getObjectId()).add(pivotId);
			}
		}

		return output;
	}

	private List<ObjectWithDistance> computeDistanceToOtherPivots(IFeatureWithPartition query, Iterable<Integer> pivotIds, List<Feature> pivots,
			int maxNumberOfBuckets) throws Exception {

		List<ObjectWithDistance> result = new ArrayList<>();
		IMetric metric = MetricProvider.getMetric();

		for (Integer pivotId : pivotIds) {
			Feature otherPivot = pivots.get(pivotId);
			result.add(new ObjectWithDistance(pivotId, metric.dist(query.getFeature(), otherPivot)));
		}

		Collections.sort(result);
		return result.subList(0, Math.min(maxNumberOfBuckets, result.size()));
	}

	/**
	 * Computes distance matrix between pivots form given group and all other pivots
	 * 
	 * @param pivots
	 * @param metric
	 * @param group
	 * @return Distance matrix.
	 * @throws IOException
	 */
	private float[][] computeDistanceMatrix(List<Feature> pivots, IMetric metric, List<Integer> group) throws IOException {

		float[][] distMatrix = new float[group.size()][pivots.size()];
		// compute the matrix
		for (int j = 0; j < group.size(); j++) {
			int pidInR = group.get(j);
			for (int pidInS = 0; pidInS < pivots.size(); pidInS++) {
				distMatrix[j][pidInS] = metric.dist(pivots.get(pidInR), pivots.get(pidInS));
			}
		}
		return distMatrix;
	}

	/**
	 * Inverts an array of pivot IDs to group IDs.
	 * 
	 * @param groupsInList
	 * @param numberOfGroups
	 * @return Groups with pivot IDs.
	 */
	private List<List<Integer>> invertGroupList(int[] groupsInList, int numberOfGroups) {

		List<List<Integer>> groups = new ArrayList<>(numberOfGroups);
		for (int i = 0; i < numberOfGroups; i++) {
			groups.add(new ArrayList<Integer>());
		}
		for (int i = 0; i < groupsInList.length; i++) {
			groups.get(groupsInList[i]).add(i);
		}
		return groups;
	}

	/**
	 * Computes upper bound for every QUERY partition present in a given group.
	 * 
	 * @return Upper bound for every query partition in the group.
	 */
	private float[] getUpperBound(List<Integer> group, float[][] distMatrix, VoronoiStatistics voronoiStatistics, int k, float epsilon) throws Exception {

		PartitionStatistics[] queryStatistics = voronoiStatistics.getQueryStatistics();
		PartitionStatistics[] databaseStatistics = voronoiStatistics.getDatabaseStatistics();
		int pivotCount = databaseStatistics.length;
		float[] ub = new float[group.size()];
		PriorityQueue<Float> knn = new PriorityQueue<Float>(k, new DescDistanceComparator());
		for (int i = 0; i < group.size(); i++) {

			int pidInR = group.get(i);
			float queryDistance = queryStatistics[pidInR].getMaxDist();
			knn.clear();
			for (int j = 0; j < pivotCount; j++) {

				PartitionStatistics databaseStats = databaseStatistics[j];
				float distBetweenPivots = distMatrix[i][j];
				for (float distToNearestNeighbor : databaseStats.getDistancesToNearestObjects()) {

					float maxDist = queryDistance + distBetweenPivots + distToNearestNeighbor;
					if (knn.size() < k) {
						knn.add(maxDist);
					} else if (maxDist < knn.peek()) {
						knn.remove();
						knn.add(maxDist);
					} else { // distances to neighbors are sorted
						break;
					}
				}
			}
			// check
			if (knn.size() != k) {
				throw new java.lang.Exception("Not enough neighbors (" + knn.size() + ") for queries in the partition " + pidInR);
			}

			ub[i] = knn.peek() / epsilon; // for exact epsilon = 1, for approx its a parameter
		}
		return ub;
	}

	/**
	 * Computes LOWER BOUND for every DATABASE partition for a given group.
	 * 
	 */
	private List<List<ObjectWithDistance>> initLBOfPartitionS(int gid, List<Integer> group, float[][] distMatrix, float[] upperBoundForR,
			List<List<ObjectWithDistance>> output, VoronoiStatistics voronoiStatistics, int[] staticPivotIds) {

		PartitionStatistics[] partS = voronoiStatistics.getDatabaseStatistics();
		PartitionStatistics[] partR = voronoiStatistics.getQueryStatistics();

		for (int pidInS = 0; pidInS < output.size(); pidInS++) {

			if (isPrunedByCutRegions(group, distMatrix, upperBoundForR, staticPivotIds, partS[pidInS].getCutRegion())) {
				continue;
			}

			float minLB = partS[pidInS].getMaxDist() + 1;
			for (int j = 0; j < group.size(); j++) {

				int pidInR = group.get(j);
				float dist = distMatrix[j][pidInS];

				// partR[pidInR].getMaxDist() maybe not necessary? - IT IS NECESSARY
				float lb = dist - partR[pidInR].getMaxDist() - upperBoundForR[j];
				if (lb < partS[pidInS].getMinDist()) {
					minLB = partS[pidInS].getMinDist();
					break;
				} else if (lb < partR[pidInR].getMaxDist() && lb < minLB) {
					minLB = lb;
				}
			}
			output.get(pidInS).add(new ObjectWithDistance(gid, minLB));
		}
		return output;
	}

	/**
	 * Computes CR pruning.
	 * 
	 */
	public boolean isPrunedByCutRegions(List<Integer> group, float[][] distMatrix, float[] upperBoundForR, int[] staticPivotIds, CutRegion cutRegion) {

		float[] distancesToStaticPivots = new float[staticPivotIds.length];

		for (int i = 0; i < group.size(); i++) {
			for (int j = 0; j < distancesToStaticPivots.length; j++) {
				distancesToStaticPivots[j] = distMatrix[i][staticPivotIds[j]];
			}

			if (cutRegion.isOverlapping(distancesToStaticPivots, upperBoundForR[i])) {
				return false;
			}
		}
		return true;
	}

}
