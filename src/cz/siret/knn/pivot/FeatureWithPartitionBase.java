package cz.siret.knn.pivot;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public abstract class FeatureWithPartitionBase implements IFeatureWithPartition {

	private final Feature feature;
	private final float distanceToPivot;
	private final boolean isDatabase; // is a query otherwise
	private final float[] distancesToStaticPivots;

	public FeatureWithPartitionBase(Feature feature, float distanceToPivot, boolean isDatabase, float[] distancesToStaticPivots) {
		this.feature = feature;
		this.distanceToPivot = distanceToPivot;
		this.isDatabase = isDatabase;
		this.distancesToStaticPivots = distancesToStaticPivots;
	}

	@Override
	public Feature getFeature() {
		return feature;
	}

	@Override
	public boolean isDatabase() {
		return isDatabase;
	}

	@Override
	public float getDistanceToPivot() {
		return distanceToPivot;
	}

	@Override
	public float[] getDistancesToStaticPivots() {
		return distancesToStaticPivots;
	}
}
