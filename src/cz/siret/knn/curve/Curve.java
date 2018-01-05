package cz.siret.knn.curve;

import java.math.BigInteger;

import cz.siret.knn.model.FeaturesKeyClassified;

@SuppressWarnings("serial")
public class Curve implements ICurveOrder {

	private final BigInteger curveValue;
	private final FeaturesKeyClassified featureKey;
	private final int dimension;
	
	public Curve(BigInteger curveValue, FeaturesKeyClassified featureKey, int dimension) {
		this.curveValue = curveValue;
		this.featureKey = featureKey;
		this.dimension = dimension;
	}
	
	@Override
	public BigInteger getCurveOrder() {	
		return curveValue;
	}

	@Override
	public Object getFeature() {
		int[] output = new int[dimension];
		ZorderHelper.toCoords(curveValue, output);
		return output;
	}

	@Override
	public int compareTo(ICurveOrder other) {
		return curveValue.compareTo(other.getCurveOrder());
	}

	@Override
	public FeaturesKeyClassified getFeatureKey() {
		return featureKey;
	}	

}
