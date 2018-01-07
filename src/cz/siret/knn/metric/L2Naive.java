package cz.siret.knn.metric;

import java.io.IOException;

import cz.siret.knn.metric.IMetric;

@SuppressWarnings("serial")
public class L2Naive extends IMetric {

	private final double shiftsScale;

	public L2Naive(int scale) {
		this.shiftsScale = scale;
	}

	@Override
	public float dist(Object o1, Object o2) throws IOException {

		if (o1 instanceof int[] && o2 instanceof int[]) {
			numOfDistComp++;
			return computeDist((int[]) o1, (int[]) o2);
		} else {
			throw new IOException("The input objects must be the type of int[]!");
		}
	}

	private float computeDist(int[] coords1, int[] coords2) {
		if (coords1.length != coords2.length)
			throw new IllegalArgumentException();

		double dist = 0;
		for (int i = 0; i < coords1.length; i++) {
			double diff = (coords1[i] - coords2[i]) / shiftsScale;
			dist += diff * diff;
		}
		return (float) Math.sqrt(dist);
	}

	@Override
	public long getNumOfDistComp() {
		return numOfDistComp;
	}

}
