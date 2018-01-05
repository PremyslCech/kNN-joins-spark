package cz.siret.knn.pivot;

import cz.siret.knn.model.Feature;
import scala.NotImplementedError;

@SuppressWarnings("serial")
public class FeatureWithOnePartition extends FeatureWithPartitionBase {

	private final int nearestPivot;

	public FeatureWithOnePartition(Feature feature, int nearestPivot, float distanceToPivot, boolean isDatabase, float[] distancesToStaticPivots) {

		super(feature, distanceToPivot, isDatabase, distancesToStaticPivots);
		this.nearestPivot = nearestPivot;
	}

	public FeatureWithOnePartition(IFeatureWithPartition feature) {

		this(feature.getFeature(), feature.getNearestPivot(), feature.getDistanceToPivot(), feature.isDatabase(), feature.getDistancesToStaticPivots());
	}

	@Override
	public int getNearestPivot() {
		return nearestPivot;
	}

	@Override
	public int[] getNearestPivots() {
		throw new NotImplementedError();
	}
}
