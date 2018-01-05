package cz.siret.knn.pivot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import cz.siret.knn.ObjectWithDistance;
import scala.Tuple2;

@SuppressWarnings("serial")
public class KNNExactCalculator extends KNNBaseCalculator {

	private final Broadcast<List<List<ObjectWithDistance>>> lowerBounds;

	public KNNExactCalculator(LongAccumulator distanceComputations, LongAccumulator databaseReplications, Broadcast<List<List<ObjectWithDistance>>> lowerBounds) {
		super(distanceComputations, databaseReplications);
		this.lowerBounds = lowerBounds;
	}

	@Override
	protected JavaPairRDD<Integer, IFeatureWithPartition> getObjectsMappedToGroups(JavaPairRDD<Integer, IFeatureWithPartition> partitionedFeatures,
			final Broadcast<int[]> groups) {

		return partitionedFeatures.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, IFeatureWithPartition>, Integer, IFeatureWithPartition>() {

			public Iterator<Tuple2<Integer, IFeatureWithPartition>> call(final Tuple2<Integer, IFeatureWithPartition> pair) throws Exception {

				if (pair._2.isDatabase()) {

					List<Tuple2<Integer, IFeatureWithPartition>> output = new ArrayList<>();
					List<List<ObjectWithDistance>> lowerBoundsLocal = lowerBounds.value();

					for (ObjectWithDistance bound : lowerBoundsLocal.get(pair._2.getNearestPivot())) {
						if (pair._2.getDistanceToPivot() < bound.getDistance()) {
							break;
						}

						int groupId = bound.getObjectId();
						output.add(new Tuple2<>(groupId, pair._2));
					}
					databaseReplications.add(output.size());
					return output.iterator();
				} else {
					return Collections.singleton(new Tuple2<>(groups.value()[pair._1], pair._2)).iterator();
				}
			}
		});
	}
}
