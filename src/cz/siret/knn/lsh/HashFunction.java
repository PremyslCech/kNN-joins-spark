package cz.siret.knn.lsh;

import java.io.Serializable;
import java.util.Random;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public class HashFunction implements Serializable{

	private double[] aVals;
	private double bVal;
	private double w;

	public HashFunction(int numDimensions, double w, Random r) {
		aVals = new double[numDimensions];
		for (int dim = 0; dim < numDimensions; dim++) {
			aVals[dim] = r.nextGaussian();
		}
		bVal = r.nextDouble() * w;
		this.w = w;
	}

	public int evaluate(Feature feature, DimensionStats[] dimensionStats) {

		double res = 0;
		for (Feature.DimValue dv : feature.iterate(aVals.length)) {
			double value = (dv.value - dimensionStats[dv.dim].getExpectedValue()) / dimensionStats[dv.dim].getStandardDeviation();
			res += value * aVals[dv.dim];
		}
		res = (res + bVal) / w;
		return (int) Math.floor(res);
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < aVals.length; i++) {
			output.append(aVals[i]).append(" ");
		}
		output.append(System.lineSeparator()).append(bVal).append(" ").append(w);
		return output.toString();
	}
}
