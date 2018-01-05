package cz.siret.knn.pivot;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public class FeatureWithMultiplePartitions extends FeatureWithPartitionBase {

	private final int[] nearestPivots;

	public FeatureWithMultiplePartitions(Feature feature, int[] nearestPivots, float distanceToPivot, boolean isDatabase, float[] distancesToStaticPivots) {
		
		super(feature, distanceToPivot, isDatabase, distancesToStaticPivots);
		this.nearestPivots = nearestPivots;
	}

	@Override
	public int getNearestPivot() {
		return nearestPivots[0];
	}

	@Override
	public int[] getNearestPivots() {
		return nearestPivots;
	}
}
