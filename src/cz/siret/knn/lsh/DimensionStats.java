package cz.siret.knn.lsh;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DimensionStats implements Serializable {

	private final double expectedValue;
	private final double standardDeviation;

	public DimensionStats(double expectedValue, double standardDeviation) {
		this.expectedValue = expectedValue;
		this.standardDeviation = standardDeviation;
	}

	public double getExpectedValue() {
		return expectedValue;
	}

	public double getStandardDeviation() {
		return standardDeviation;
	}
}
