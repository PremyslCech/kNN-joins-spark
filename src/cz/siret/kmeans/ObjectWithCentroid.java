package cz.siret.kmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ObjectWithCentroid implements Serializable {

	private final String id;
	private final int centroidId;

	public ObjectWithCentroid(String id, int centroidId) {
		this.id = id;
		this.centroidId = centroidId;
	}

	public String getId() {
		return id;
	}

	public int getCentroidId() {
		return centroidId;
	}

	@Override
	public String toString() {
		return id + ";" + centroidId;
	}
}
