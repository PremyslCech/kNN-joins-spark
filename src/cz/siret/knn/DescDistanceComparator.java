package cz.siret.knn;

import java.util.Comparator;

public class DescDistanceComparator implements Comparator<Float> {

	public int compare(Float o1, Float o2) {
		return -Float.compare(o1, o2); // descending
	}
}
