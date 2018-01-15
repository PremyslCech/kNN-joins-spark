package cz.siret.knn.pivot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

@SuppressWarnings("serial")
public class KNNEpsApproxCalculator extends KNNBaseCalculator {

	private final Broadcast<float[]> upperBounds;
	private final Broadcast<VoronoiStatistics> voronoiStatistics;

	public KNNEpsApproxCalculator(LongAccumulator distanceComputations, LongAccumulator databaseReplications, Broadcast<float[]> upperBounds,
			Broadcast<VoronoiStatistics> voronoiStatistics) {
		super(distanceComputations, databaseReplications);
		this.upperBounds = upperBounds;
		this.voronoiStatistics = voronoiStatistics;
	}

	@Override
	protected JavaPairRDD<Integer, IFeatureWithPartition> getObjectsMappedToGroups(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures,
			final Broadcast<int[]> groups) {

		return partitionedFeatures.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, IFeatureWithPartition>, Integer, IFeatureWithPartition>() {

			public Iterator<Tuple2<Integer, IFeatureWithPartition>> call(final Tuple2<Integer, IFeatureWithPartition> pair) throws Exception {

				final int[] groupsLocal = groups.value();
				if (pair._2.isDatabase()) {

					float[] ubLocal = upperBounds.value();
					PartitionStatistics[] queryStatsLocal = voronoiStatistics.value().getQueryStatistics();
					HashSet<Integer> usedGroups = new HashSet<>();
					// copy without all nearest neighbors which are not needed later
					IFeatureWithPartition outputFeature = new FeatureWithOnePartition(pair._2);
					List<Tuple2<Integer, IFeatureWithPartition>> output = new ArrayList<>();

					int[] nearestPivots = pair._2.getNearestPivots();
					float[] distToNearestPivots = pair._2.getDistToNearestPivots();
					for (int i = 0; i < nearestPivots.length; i++) {
						int nearestPivot = nearestPivots[i];
						int groupId = groupsLocal[nearestPivot];
						if (usedGroups.contains(groupId)) { // DB object is already replicated to this group
							continue;
						}

						float lb = distToNearestPivots[i] - queryStatsLocal[nearestPivot].getMaxDist();
						if (lb > ubLocal[nearestPivot]) {
							continue;
						}
						
						usedGroups.add(groupId);
						output.add(new Tuple2<Integer, IFeatureWithPartition>(groupId, outputFeature));
					}
					databaseReplications.add(output.size());
					return output.iterator();

				} else {
					return Collections.singleton(new Tuple2<Integer, IFeatureWithPartition>(groupsLocal[pair._1], new FeatureWithOnePartition(pair._2))).iterator();
				}
			}
		});
	}
}
