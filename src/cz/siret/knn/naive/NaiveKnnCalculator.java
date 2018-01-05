package cz.siret.knn.naive;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import cz.siret.knn.KnnHelper;
import cz.siret.knn.metric.IMetric;
import cz.siret.knn.metric.MetricProvider;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;
import cz.siret.knn.model.FeaturesKeyClassified;
import cz.siret.knn.model.NNResult;
import scala.Tuple2;

@SuppressWarnings("serial")
public class NaiveKnnCalculator implements Serializable {

	public JavaRDD<NNResult> calculateKnn(JavaPairRDD<Iterable<Feature>, Iterable<Feature>> candidateBuckets, final int k) {

		return mergeResults(getIntermediateResults(candidateBuckets, k), k);
	}

	private JavaPairRDD<FeaturesKey, NNResult> getIntermediateResults(
			JavaPairRDD<Iterable<Feature>, Iterable<Feature>> candidateBuckets, final int k) {

		return candidateBuckets
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<Feature>, Iterable<Feature>>, FeaturesKey, NNResult>() {

					public Iterator<Tuple2<FeaturesKey, NNResult>> call(final Tuple2<Iterable<Feature>, Iterable<Feature>> group)
							throws Exception {

						return new Iterator<Tuple2<FeaturesKey, NNResult>>() {

							private final KnnHelper knnHelper = new KnnHelper();
							private final List<Feature> databaseObjects = new ArrayList<>();
							private final Iterator<Feature> queryIterator = group._1.iterator();
							private final PriorityQueue<Tuple2<FeaturesKeyClassified, Float>> knn;
							{
								knn = knnHelper.getNeighborsPriorityQueue(k);
								for (Feature featureWritable : group._2) {
									databaseObjects.add(featureWritable);
								}
							}

							@Override
							public boolean hasNext() {
								return queryIterator.hasNext();
							}

							@Override
							public Tuple2<FeaturesKey, NNResult> next() {

								final IMetric metric = MetricProvider.getMetric();
								knn.clear();
								Feature query = queryIterator.next();
								for (Feature databaseObject : databaseObjects) {

									try {
										knn.add(new Tuple2<>(databaseObject.toFeaturesKeyClassified(), metric.dist(query, databaseObject)));
									} catch (IOException e) {
										e.printStackTrace();
									}
									if (knn.size() > k) {
										knn.remove();
									}
								}
								return new Tuple2<>(query.getKey(), knnHelper.getNeighborsResult(query.toFeaturesKeyClassified(), knn));
							}
						};

					}
				});
	}

	public JavaRDD<NNResult> mergeResults(JavaPairRDD<FeaturesKey, NNResult> intermediateResults, int k) {

		return intermediateResults.reduceByKey(getMergeFunction(k)).values();
	}

	private Function2<NNResult, NNResult, NNResult> getMergeFunction(final int k) {

		return new Function2<NNResult, NNResult, NNResult>() {

			public NNResult call(NNResult nn1, NNResult nn2) throws Exception {

				if (nn1.queryClassification != nn2.queryClassification || !nn1.queryKey.equals(nn2.queryKey)) {
					throw new Exception("Queries are not compatible!");
				}
				checkNNResult(nn1);
				checkNNResult(nn2);
				if (nn1.dbDistances.size() == 0) {
					return nn2;
				} else if (nn2.dbDistances.size() == 0) {
					return nn1;
				}

				NNResult output = new NNResult();
				output.queryClassification = nn1.queryClassification;
				output.queryKey = nn1.queryKey;

				NNResult nearerNeighbor = nn1.dbDistances.get(0) < nn2.dbDistances.get(0) ? nn1 : nn2;
				int index1 = 0, index2 = 0, actualIndex = 0;

				while (output.dbKeys.size() < k && (index1 < nn1.dbKeys.size() || index2 < nn2.dbKeys.size())) {

					if (!output.dbKeys.contains(nearerNeighbor.dbKeys.get(actualIndex))) {
						output.dbClassifications.add(nearerNeighbor.dbClassifications.get(actualIndex));
						output.dbDistances.add(nearerNeighbor.dbDistances.get(actualIndex));
						output.dbKeys.add(nearerNeighbor.dbKeys.get(actualIndex));
					}

					if (nearerNeighbor == nn1) {
						actualIndex = ++index1;
						if (nn1.dbDistances.size() <= index1 || (nn2.dbDistances.size() > index2 && nn1.dbDistances.get(index1) > nn2.dbDistances.get(index2))) {
							nearerNeighbor = nn2;
							actualIndex = index2;
						}
					} else {
						actualIndex = ++index2;
						if (nn2.dbDistances.size() <= index2 || (nn1.dbDistances.size() > index1 && nn1.dbDistances.get(index1) < nn2.dbDistances.get(index2))) {
							nearerNeighbor = nn1;
							actualIndex = index1;
						}
					}
				}

				return output;
			}

			private void checkNNResult(NNResult nn1) throws Exception {
				if (nn1.dbClassifications.size() != nn1.dbDistances.size() || nn1.dbClassifications.size() != nn1.dbKeys.size()) {
					throw new Exception("Weird NN results input for merge!");
				}
			}

		};
	}

}
