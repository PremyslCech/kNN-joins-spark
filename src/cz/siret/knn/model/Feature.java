package cz.siret.knn.model;

import java.io.Serializable;
import java.util.*;

@SuppressWarnings("serial")
public class Feature implements Comparable<Feature>, Serializable {

	private FeaturesKey key;
	private int classification;
	private float otherBinsValue;
	private int[] binKeys;
	private float[] binValues;

	private Feature(FeaturesKey key, int classification, float otherBinsValue) {
		this.key = key;
		this.classification = classification;
		this.otherBinsValue = otherBinsValue;

	}

	public Feature() {
	}

	public Feature(FeaturesKey key, int classification, float otherBinsValue, ArrayList<Pair> values) {

		this(key, classification, otherBinsValue);

		int[] binKeys = new int[values.size()];
		float[] binValues = new float[values.size()];
		for (int i = 0; i < values.size(); i++) {
			binKeys[i] = values.get(i).getKey();
			binValues[i] = values.get(i).getValue();
		}
		this.binKeys = binKeys;
		this.binValues = binValues;
	}
	
	public Feature(FeaturesKey key, int classification, float otherBinsValue, int[] keys, float[] values) {

		this(key, classification, otherBinsValue);

		this.binKeys = keys;
		this.binValues = values;
	}

	@Override
	public String toString() { /* CISCO test */
		StringBuilder result = new StringBuilder();
		result.append(key.toString());
		result.append(';');
		result.append(classification);
		result.append(';');
		result.append(otherBinsValue);
		result.append(';');
		for (int i = 0; i < binKeys.length; i++) {
			result.append(binKeys[i]);
			result.append(':');
			result.append(binValues[i]);
			result.append(';');
		}
		result.deleteCharAt(result.length() - 1);
		return result.toString();
		/*
		 * Map<Integer, Float> map = new HashMap<>();
		 * 
		 * float total = 0; for(PairWritable pair : valuesInArray) { map.put(pair.getKey(), pair.getValue()); total += pair.getValue(); }
		 * 
		 * StringBuilder s = new StringBuilder(); for(int i=0; i<400; i++) { Float value = map.get(i); if(value != null) s.append(value/total); else s.append(0);
		 * 
		 * if(i<399) s.append(","); } return s.toString();
		 */
	}

	private final static int idIndex = 0;
	private final static int classificationIndex = 1;
	private final static int otherBinsValueIndex = 2;

	public static Feature parse(String line) {
		try {
			String[] parts = line.split(";");
			FeaturesKey keyWritable = FeaturesKey.parse(parts[idIndex]);
			int classification = Integer.parseInt(parts[classificationIndex]);
			float otherBinsValue = Float.parseFloat(parts[otherBinsValueIndex]);

			ArrayList<Pair> pairs = new ArrayList<>();
			for (int i = otherBinsValueIndex + 1; i < parts.length; i++) {
				String[] pair = parts[i].split(":");
				int key = Integer.parseInt(pair[0]);
				float value = Float.parseFloat(pair[1]);
				pairs.add(new Pair(key, value));
			}
			return new Feature(keyWritable, classification, otherBinsValue, pairs);
		} catch (Exception e) {
			throw new RuntimeException("Could not parse line '" + line + "'");
		}
	}

	public FeaturesKey getKey() {
		return key;
	}

	public int getClassification() {
		return classification;
	}

	public float getOtherBinsValue() {
		return otherBinsValue;
	}

	public int[] getKeys() {
		return binKeys;
	}
	
	public float[] getValues() {
		return binValues;
	}

	public FeaturesKeyClassified toFeaturesKeyClassified() {
		return new FeaturesKeyClassified(key.getFiveMinuteId(), key.getClientId(), classification);
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	/** we consider FeatureWritables equal when the key is equal */
	@Override
	public boolean equals(Object o) {
		if (o instanceof Feature) {
			Feature f = (Feature) o;
			return key.equals(f.key);
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(Feature o) {
		return getKey().compareTo(o.getKey());
	}

	public Iterable<DimValue> iterate(int numDims) {
		return new FeatureIterable(numDims);
	}

	public static class DimValue {
		public int dim;
		public float value;
	}

	private class FeatureIterable implements Iterable<DimValue> {

		int numDims;

		FeatureIterable(int numDims) {
			this.numDims = numDims;
		}

		public Iterator<DimValue> iterator() {
			return new FeatureIterator(numDims);
		}
	}

	private class FeatureIterator implements Iterator<DimValue> {

		int currentDim = 0;
		int numDims;
		int valuesInArrayPos = 0;
		DimValue dv = new DimValue();

		public FeatureIterator(int numDims) {
			this.numDims = numDims;
		}

		@Override
		public boolean hasNext() {
			return currentDim < numDims;
		}

		@Override
		public DimValue next() {
			if (valuesInArrayPos >= binKeys.length)
				dv.value = otherBinsValue;
			else if (binKeys[valuesInArrayPos] == currentDim)
				dv.value = binValues[valuesInArrayPos++];
			else if (binKeys[valuesInArrayPos] < currentDim)
				throw new Error("PairWritables not sorted by dimension");
			else
				dv.value = otherBinsValue;

			dv.dim = currentDim++;

			return dv;
		}

		@Override
		public void remove() {
			throw new Error("remove not implemented");
		}
	}

	public static void main(String[] args) {
		FeaturesKey fkw = new FeaturesKey(1, 2);
		FeaturesKey fkw2 = new FeaturesKey(2, 2);
		ArrayList<Pair> pw = new ArrayList<>();
		pw.add(new Pair(0, 0F));
		pw.add(new Pair(1, 1.0F));
		pw.add(new Pair(5, 5.0F));
		Feature fw = new Feature(fkw, 0, 666.0F, pw);
		Feature fw2 = new Feature(fkw2, 1, 676.0F, pw);
		
		System.out.println(fw);
		System.out.println(fw2);
		
		System.out.println(Feature.parse("2_2;1;676.0;0:0.0;1:1.0;5:5.0"));
		System.out.println("Iterating..");
		for (DimValue dv : Feature.parse("2_2;1;676.0;0:0.0;1:1.0;5:5.0").iterate(7)) {
			System.out.print(dv.dim + ":" + dv.value + "  ");
		}
		System.out.println();

		Map<Feature, Integer> m = new HashMap<>();
		m.put(fw, 1);
		m.put(fw2, 2);

		for (Map.Entry<Feature, Integer> e : m.entrySet())
			System.out.println(e.getKey().getKey() + " " + e.getValue());
	}
}
