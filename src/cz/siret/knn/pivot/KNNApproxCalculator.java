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
public class KNNApproxCalculator extends KNNBaseCalculator {

	private final int maxNumberOfBuckets;

	public KNNApproxCalculator(LongAccumulator distanceComputations, LongAccumulator databaseReplications, int maxNumberOfBuckets) {
		super(distanceComputations, databaseReplications);
		this.maxNumberOfBuckets = maxNumberOfBuckets;
	}

	@Override
	protected JavaPairRDD<Integer, IFeatureWithPartition> getObjectsMappedToGroups(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures,
			final Broadcast<int[]> groups) {

		return partitionedFeatures.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, IFeatureWithPartition>, Integer, IFeatureWithPartition>() {

			public Iterator<Tuple2<Integer, IFeatureWithPartition>> call(final Tuple2<Integer, IFeatureWithPartition> pair) throws Exception {

				final int[] groupsLocal = groups.value();
				if (pair._2.isDatabase()) {

					HashSet<Integer> usedGroups = new HashSet<>();
					// copy without all nearest neighbors which are not needed later
					IFeatureWithPartition outputFeature = new FeatureWithOnePartition(pair._2);
					List<Tuple2<Integer, IFeatureWithPartition>> output = new ArrayList<>();

					int[] nearestPivots = pair._2.getNearestPivots();
					for (int nearestPivot : nearestPivots) {
						int groupId = groupsLocal[nearestPivot];
						if (usedGroups.add(groupId)) {
							output.add(new Tuple2<Integer, IFeatureWithPartition>(groupId, outputFeature));
						}
					}
					databaseReplications.add(output.size());
					return output.iterator();

				} else {
					return Collections.singleton(new Tuple2<Integer, IFeatureWithPartition>(groupsLocal[pair._1], new FeatureWithOnePartition(pair._2))).iterator();
				}
			}
		});
	}
	
	@Override
	protected boolean TerminateEarly(int orderOfPartition) {
		// only first "maxNumberOfBuckets" partitions are visited
		return orderOfPartition >= maxNumberOfBuckets;
	}
}
