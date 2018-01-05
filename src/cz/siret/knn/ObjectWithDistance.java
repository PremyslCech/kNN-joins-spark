package cz.siret.knn;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ObjectWithDistance implements Comparable<ObjectWithDistance>, Serializable {

	private final int objectId;
	private final float distance;

	public ObjectWithDistance(int objectId, float distance) {
		this.objectId = objectId;
		this.distance = distance;
	}

	public int getObjectId() {
		return objectId;
	}

	public float getDistance() {
		return distance;
	}

	@Override
	public int compareTo(ObjectWithDistance other) {
		return Float.compare(distance, other.distance);
	}
}
