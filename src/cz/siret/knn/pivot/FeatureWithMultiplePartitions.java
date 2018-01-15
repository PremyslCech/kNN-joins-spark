package cz.siret.knn.pivot;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public class FeatureWithMultiplePartitions extends FeatureWithPartitionBase {

	private final int[] nearestPivots;
	private final float[] distToNearestPivots;

	public FeatureWithMultiplePartitions(Feature feature, int[] nearestPivots, float[] distToNearestPivots, float distanceToPivot, boolean isDatabase,
			float[] distancesToStaticPivots) {

		super(feature, distanceToPivot, isDatabase, distancesToStaticPivots);
		this.nearestPivots = nearestPivots;
		this.distToNearestPivots = distToNearestPivots;
	}

	@Override
	public int getNearestPivot() {
		return nearestPivots[0];
	}

	@Override
	public int[] getNearestPivots() {
		return nearestPivots;
	}

	@Override
	public float[] getDistToNearestPivots() {
		// TODO Auto-generated method stub
		return distToNearestPivots;
	}
}
