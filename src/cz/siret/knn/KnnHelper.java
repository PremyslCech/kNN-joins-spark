package cz.siret.knn;

import java.util.Comparator;
import java.util.PriorityQueue;

import cz.siret.knn.model.FeaturesKey;
import cz.siret.knn.model.FeaturesKeyClassified;
import cz.siret.knn.model.NNResult;
import scala.Tuple2;

public class KnnHelper {

	public PriorityQueue<Tuple2<FeaturesKeyClassified, Float>> getNeighborsPriorityQueue(final int k) {

		return new PriorityQueue<>(k, new Comparator<Tuple2<FeaturesKeyClassified, Float>>() {

			public int compare(Tuple2<FeaturesKeyClassified, Float> o1, Tuple2<FeaturesKeyClassified, Float> o2) {
				int compare;
				if ((compare = Float.compare(o1._2, o2._2)) != 0) {
					return -compare; // we want DESCENDING order
				}
				return o1._1.compareTo(o2._1);
			}
		});
	}

	public NNResult getNeighborsResult(FeaturesKeyClassified queryKey, PriorityQueue<Tuple2<FeaturesKeyClassified, Float>> neighbors) {

		NNResult result = new NNResult();
		result.queryKey = new FeaturesKey(queryKey);
		result.queryClassification = queryKey.getClassification();

		Tuple2<FeaturesKeyClassified, Float> neighbor;
		while ((neighbor = neighbors.poll()) != null) {
			// we need to insert items in the beginning of arrays because queue is in descending order
			result.dbKeys.add(0, new FeaturesKey(neighbor._1));
			result.dbDistances.add(0, neighbor._2);
			result.dbClassifications.add(0, neighbor._1.getClassification());
		}
		return result;
	}
}
