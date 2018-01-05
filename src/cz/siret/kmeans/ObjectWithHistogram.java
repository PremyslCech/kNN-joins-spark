package cz.siret.kmeans;

import java.io.Serializable;

@SuppressWarnings("serial")
public class ObjectWithHistogram implements Serializable {

	private final String id;
	private final float[] histogram;
	
	public ObjectWithHistogram(String id, float[] histogram) {
		this.id = id;
		this.histogram = histogram;
	}

	public String getId() {
		return id;
	}

	public float[] getHistogram() {
		return histogram;
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(id);
		output.append(";");
		for (float value : histogram) {
			output.append(value);
			output.append(";");
		}		
		output.deleteCharAt(output.length() - 1);
		return output.toString();
	}
}
