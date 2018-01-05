package cz.siret.knn.lsh;

import java.io.Serializable;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public class HashTable implements Serializable {

	private final HashFunction[] hashFunctions;

	public HashTable(HashFunction[] hashFunctions) {
		this.hashFunctions = hashFunctions;
	}

	public int[] evaluate(Feature feature, DimensionStats[] dimensionStats) {

		int[] res = new int[hashFunctions.length];
		for (int i = 0; i < hashFunctions.length; i++) {
			res[i] = hashFunctions[i].evaluate(feature, dimensionStats);
		}
		return res;
	}

	@Override
	public String toString() {

		StringBuilder output = new StringBuilder();
		for (int i = 0; i < hashFunctions.length; i++) {
			output.append(hashFunctions[i].toString());
			if (i != hashFunctions.length - 1) {
				output.append(System.lineSeparator());
			}
		}
		return output.toString();
	}

}