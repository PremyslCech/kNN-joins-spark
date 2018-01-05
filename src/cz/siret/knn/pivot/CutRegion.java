package cz.siret.knn.pivot;

public class CutRegion {

	private final float[] minDistances;
	private final float[] maxDistances;

	public CutRegion(int staticPivotCount) {
		minDistances = new float[staticPivotCount];
		for (int i = 0; i < minDistances.length; i++) {
			minDistances[i] = Float.MAX_VALUE;
		}
		maxDistances = new float[staticPivotCount];
		for (int i = 0; i < maxDistances.length; i++) {
			maxDistances[i] = -Float.MAX_VALUE;
		}
	}

	public CutRegion(int[] pivotIds, float[] minDistances, float[] maxDistances) {
		this.minDistances = minDistances;
		this.maxDistances = maxDistances;
	}

	public float[] getMinDistances() {
		return minDistances;
	}

	public float[] getMaxDistances() {
		return maxDistances;
	}

	public void addFeature(IFeatureWithPartition feature) {

		float[] distancesToStaticPivots = feature.getDistancesToStaticPivots();
		for (int i = 0; i < minDistances.length; i++) {
			if (minDistances[i] > distancesToStaticPivots[i]) {
				minDistances[i] = distancesToStaticPivots[i];
			}
			if (maxDistances[i] < distancesToStaticPivots[i]) {
				maxDistances[i] = distancesToStaticPivots[i];
			}
		}
	}
	
	public void merge(CutRegion otherCutRegion) {
		
		for (int i = 0; i < minDistances.length; i++) {
			if (minDistances[i] > otherCutRegion.minDistances[i]) {
				minDistances[i] = otherCutRegion.minDistances[i];
			}
			if (maxDistances[i] < otherCutRegion.maxDistances[i]) {
				maxDistances[i] = otherCutRegion.maxDistances[i];
			}
		}
	}

	public boolean isOverlapping(float[] distancesToStaticPivots , float radius) {

		for (int i = 0; i < minDistances.length; i++) {
			if (distancesToStaticPivots[i] + radius < minDistances[i] || distancesToStaticPivots[i] - radius > maxDistances[i]) {
				return false;
			}
		}

		return true;
	}
}
