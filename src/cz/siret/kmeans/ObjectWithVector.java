package cz.siret.kmeans;

import java.io.Serializable;

import org.apache.spark.mllib.linalg.Vector;

@SuppressWarnings("serial")
public class ObjectWithVector implements Serializable {

	private final int id;	
	private final String coordinates;
	private final Vector vector;
	
	public ObjectWithVector(int id, String coordinates, Vector vector) {
		this.id = id;
		this.coordinates = coordinates;
		this.vector = vector;
	}

	public int getId() {
		return id;
	}

	public Vector getVector() {
		return vector;
	}

	public String getCoordinates() {
		return coordinates;
	}
	
}
