package cz.siret.knn.lsh;

import java.io.Serializable;
import java.util.Arrays;

@SuppressWarnings("serial")
public class HashPartition implements Serializable {

	public final int tableId;
	public final int[] hashValue;

	public HashPartition(int tableId, int[] hashValue) {
		this.tableId = tableId;
		this.hashValue = hashValue;
	}

	@Override
	public int hashCode() {
		return tableId * 37 + Arrays.hashCode(hashValue);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof HashPartition)) {
			return false;
		}

		HashPartition other = (HashPartition) obj;
		if (tableId != other.tableId || hashValue.length != other.hashValue.length) {
			return false;
		}

		for (int i = 0; i < hashValue.length; i++) {
			if (hashValue[i] != other.hashValue[i]) {
				return false;
			}
		}

		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(tableId).append(" ");
		for (int value : hashValue) {
			output.append(value).append(";");
		}
		output.deleteCharAt(output.length() - 1);
		return output.toString();
	}
}
