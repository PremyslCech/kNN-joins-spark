package cz.siret.knn.pivot;

import java.io.Serializable;

@SuppressWarnings("serial")
public class PartitionStatistics implements Serializable {

	private float minDist;
	private float maxDist;
	private int numberOfObjects;
	private long sizeOfObjects;

	private float[] distancesToNearestObjects;

	private final CutRegion cutRegion;

	public PartitionStatistics() {
		this(0, 0, 0, 0, new float[0], new CutRegion(0));
	}

	public PartitionStatistics(float minDist, float maxDist, int numberOfObjects, long sizeOfObjects) {
		this(minDist, maxDist, numberOfObjects, sizeOfObjects, null, null);
	}

	public PartitionStatistics(float minDist, float maxDist, int numberOfObjects, long sizeOfObjects, float[] distancesToNearestObjects, CutRegion cutRegion) {
		this.minDist = minDist;
		this.maxDist = maxDist;
		this.numberOfObjects = numberOfObjects;
		this.sizeOfObjects = sizeOfObjects;
		this.distancesToNearestObjects = distancesToNearestObjects;
		this.cutRegion = cutRegion;
	}

	public float getMinDist() {
		return minDist;
	}

	public float getMaxDist() {
		return maxDist;
	}

	public int getNumberOfObjects() {
		return numberOfObjects;
	}

	public long getSizeOfObjects() {
		return sizeOfObjects;
	}

	public float[] getDistancesToNearestObjects() {
		return distancesToNearestObjects;
	}

	public CutRegion getCutRegion() {
		return cutRegion;
	}
}
