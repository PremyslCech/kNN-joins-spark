package cz.siret.knn.pivot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import cz.siret.knn.DescDistanceComparator;
import cz.siret.knn.KnnHelper;
import cz.siret.knn.ObjectWithDistance;
import cz.siret.knn.metric.IMetric;
import cz.siret.knn.metric.MetricProvider;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKeyClassified;
import cz.siret.knn.model.NNResult;
import scala.Tuple2;

@SuppressWarnings("serial")
public abstract class KNNBaseCalculator implements Serializable {

	private final LongAccumulator distanceComputations;
	protected final LongAccumulator databaseReplications;

	public KNNBaseCalculator(LongAccumulator distanceComputations, LongAccumulator databaseReplications) {
		this.distanceComputations = distanceComputations;
		this.databaseReplications = databaseReplications;
	}

	/**
	 * Computes final neighbors for all queries using different strategies.
	 */
	public JavaRDD<NNResult> computeKNN(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures, final int k, final Broadcast<List<Feature>> pivots,
			final Broadcast<VoronoiStatistics> voronoiStats, final Broadcast<int[]> groups, final int numberOfReducers, final float epsilon,
			final boolean exactParentFiltering) {

		return getObjectsMappedToGroups(partitionedFeatures, groups).groupByKey(numberOfReducers)
				.flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<IFeatureWithPartition>>, NNResult>() {

					public Iterator<NNResult> call(Tuple2<Integer, Iterable<IFeatureWithPartition>> group) throws Exception {

						// parse incoming objects to groups
						final HashMap<Integer, List<IFeatureWithPartition>> partsR = new HashMap<>();
						final HashMap<Integer, List<IFeatureWithPartition>> partsS = new HashMap<>();
						for (IFeatureWithPartition feature : group._2) {

							int pivotId = feature.getNearestPivot();
							if (feature.isDatabase()) {

								if (!partsS.containsKey(pivotId)) {
									partsS.put(pivotId, new ArrayList<IFeatureWithPartition>());
								}
								partsS.get(pivotId).add(feature);
							} else {

								if (!partsR.containsKey(pivotId)) {
									partsR.put(pivotId, new ArrayList<IFeatureWithPartition>());
								}
								partsR.get(pivotId).add(feature);
							}
						}

						// sorting database objects - break instead of continue can be used in final kNN evaluation
						for (List<IFeatureWithPartition> partS : partsS.values()) {
							Collections.sort(partS, new Comparator<IFeatureWithPartition>() {

								public int compare(IFeatureWithPartition o1, IFeatureWithPartition o2) {
									int compare;
									if ((compare = Float.compare(o1.getDistanceToPivot(), o2.getDistanceToPivot())) != 0) {
										return -compare; // descending
									}
									return o1.getFeature().getKey().compareTo(o2.getFeature().getKey());
								}
							});
						}

						final IMetric metric = MetricProvider.getNewMetric();
						final List<Feature> pivotsLocal = pivots.value();
						final PartitionStatistics[] databasePartitionStats = voronoiStats.value().getDatabaseStatistics();

						return getKnnResultsIterator(partsR, partsS, metric, pivotsLocal, databasePartitionStats, k, epsilon, exactParentFiltering);
					}
				});
	}

	protected abstract JavaPairRDD<Integer, IFeatureWithPartition> getObjectsMappedToGroups(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures,
			Broadcast<int[]> groups);

	private Iterator<NNResult> getKnnResultsIterator(final HashMap<Integer, List<IFeatureWithPartition>> partsR,
			final HashMap<Integer, List<IFeatureWithPartition>> partsS, final IMetric metric, final List<Feature> pivotsLocal,
			final PartitionStatistics[] databasePartitionStats, final int k, final float epsilon, final boolean exactParentFiltering) {

		return new Iterator<NNResult>() {

			int pivotId = 0, index = 0;
			int pivotSize = pivotsLocal.size();
			float[] distancesBetweenPivots;

			// initialization
			{
				findNextPartition();
			}

			private void findNextPartition() {
				while (!partsR.containsKey(pivotId) && pivotId < pivotSize) {
					pivotId++;
				}
				// end of iterating
				if (pivotId >= pivotSize) {
					distanceComputations.add(metric.numOfDistComp);
				}
			}

			public boolean hasNext() {
				return pivotId < pivotSize && index < partsR.get(pivotId).size();
			}

			public NNResult next() {
				List<IFeatureWithPartition> queries = partsR.get(pivotId);
				IFeatureWithPartition query = queries.get(index++);
				try {
					if (distancesBetweenPivots == null) {
						distancesBetweenPivots = computeDistanceBetweenPivots(query.getNearestPivot(), pivotsLocal, metric);
					}

					// perform main k-NN join using metric space pruning techniques
					NNResult output = findKNNForSingleObject(query, k, partsS, metric, pivotsLocal, databasePartitionStats, distancesBetweenPivots, epsilon,
							exactParentFiltering);

					if (index >= queries.size()) {
						pivotId++;
						index = 0;
						distancesBetweenPivots = null;
						findNextPartition();
					}

					return output;
				} catch (Exception e) {
					e.printStackTrace();
					return null;
				}
			}
		};
	}

	private float[] computeDistanceBetweenPivots(int queryPivotId, List<Feature> pivots, IMetric metric) throws Exception {
		float[] output = new float[pivots.size()];
		for (int i = 0; i < pivots.size(); i++) {
			output[i] = metric.dist(pivots.get(queryPivotId), pivots.get(i));
		}
		return output;
	}

	private NNResult findKNNForSingleObject(IFeatureWithPartition query, final int k, HashMap<Integer, List<IFeatureWithPartition>> partsS, IMetric metric,
			List<Feature> pivots, PartitionStatistics[] databasePartitionStats, float[] distancesBetweenPivots, float epsilon, boolean exactParentFiltering)
			throws Exception {

		KnnHelper knnHelper = new KnnHelper();
		PriorityQueue<Tuple2<FeaturesKeyClassified, Float>> neighbors = knnHelper.getNeighborsPriorityQueue(k);
		List<ObjectWithDistance> distanceToOtherPivots = computeDistanceToOtherPivots(query, partsS, metric, pivots, databasePartitionStats);
		float actualRadius = getMaxRadius(query, distanceToOtherPivots, databasePartitionStats, k);
		float ballFilterRadius = actualRadius; // the epsilon is used after "k" candidates are found
		for (int i = 0; i < distanceToOtherPivots.size(); i++) {

			// possible approximation
			if (TerminateEarly(i)) {
				break;
			}

			int pid = distanceToOtherPivots.get(i).getObjectId();
			float distFromQueryToPivot = distanceToOtherPivots.get(i).getDistance();

			/** compute and update */
			if (databasePartitionStats == null) {
				throw new Error("databasePartitionStats is null");
			} else if (databasePartitionStats[pid] == null) {
				throw new Error("databasePartitionStats for pivot id " + pid + " is null");
			}

			// !!works only for some metrics!!
			// float distToHP = (distFromQueryToPivot * distFromQueryToPivot - query.getDistanceToPivot() * query.getDistanceToPivot())
			// / (2 * distancesBetweenPivots[pid]);
			// distToHP > ballFilterRadius

			if (distFromQueryToPivot - ballFilterRadius >= query.getDistanceToPivot() + ballFilterRadius // hyperplane filtering
					|| distFromQueryToPivot > databasePartitionStats[pid].getMaxDist() + ballFilterRadius // ball filtering
					|| !databasePartitionStats[pid].getCutRegion().isOverlapping(query.getDistancesToStaticPivots(), ballFilterRadius)) { // cut-region filtering
				continue;
			}

			List<IFeatureWithPartition> partFromS = partsS.get(pid);
			for (IFeatureWithPartition o_S : partFromS) {

				if (distFromQueryToPivot > o_S.getDistanceToPivot() + actualRadius) {
					break; // database objects are sorted
				}
				// lower bound filtering
				if (Math.abs(distFromQueryToPivot - o_S.getDistanceToPivot()) > actualRadius) {
					continue;
				}

				float dist = metric.dist(query.getFeature(), o_S.getFeature());
				if (neighbors.size() < k) {
					neighbors.add(new Tuple2<>(o_S.getFeature().toFeaturesKeyClassified(), dist));
					if (neighbors.size() == k) {
						actualRadius = neighbors.peek()._2 / (exactParentFiltering ? 1 : epsilon); // the epsilon is used after "k" candidates are found
						ballFilterRadius = neighbors.peek()._2 / epsilon;
					}
				} else if (dist < neighbors.peek()._2) {
					neighbors.remove();
					neighbors.add(new Tuple2<>(o_S.getFeature().toFeaturesKeyClassified(), dist));
					actualRadius = neighbors.peek()._2 / (exactParentFiltering ? 1 : epsilon);
					ballFilterRadius = neighbors.peek()._2 / epsilon;
				}
			}
		}

		return knnHelper.getNeighborsResult(query.getFeature().toFeaturesKeyClassified(), neighbors);
	}

	private List<ObjectWithDistance> computeDistanceToOtherPivots(IFeatureWithPartition query, HashMap<Integer, List<IFeatureWithPartition>> partsS,
			IMetric metric, List<Feature> pivots, final PartitionStatistics[] databasePartitionStats) throws Exception {

		List<ObjectWithDistance> result = new ArrayList<>(partsS.size());

		for (Integer pivotId : partsS.keySet()) {
			Feature otherPivot = pivots.get(pivotId);
			result.add(new ObjectWithDistance(pivotId, metric.dist(query.getFeature(), otherPivot)));
		}

		//Collections.sort(result);
		Collections.sort(result, new Comparator<ObjectWithDistance>() {
			public int compare(ObjectWithDistance o1, ObjectWithDistance o2) {
				float dist1 = o1.getDistance() - databasePartitionStats[o1.getObjectId()].getMaxDist();
				float dist2 = o2.getDistance() - databasePartitionStats[o2.getObjectId()].getMaxDist();
				return Float.compare(dist1, dist2);
			}
		});
		return result;
	}

	private float getMaxRadius(IFeatureWithPartition query, List<ObjectWithDistance> distanceToPivots, PartitionStatistics[] databasePartitions, int k) {

		PriorityQueue<Float> distances = new PriorityQueue<>(k, new DescDistanceComparator());

		for (ObjectWithDistance queryToPivot : distanceToPivots) {

			PartitionStatistics stats = databasePartitions[queryToPivot.getObjectId()];
			for (float radius : stats.getDistancesToNearestObjects()) {

				float maxDist = radius + queryToPivot.getDistance();
				if (distances.size() < k) {
					distances.add(maxDist);
				} else if (maxDist < distances.peek()) {
					distances.remove();
					distances.add(maxDist);
				} else {
					break;
				}
			}
		}

		return distances.peek();
	}

	protected boolean TerminateEarly(int orderOfPartition) {
		return false;
	}
}
