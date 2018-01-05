package cz.siret.knn.curve;

import java.math.BigInteger;

import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKeyClassified;

@SuppressWarnings("serial")
public class CurveWithFeature implements ICurveOrder {

	private final BigInteger curveValue;
	private final Feature feature;

	public CurveWithFeature(BigInteger curveValue, Feature feature) {
		this.curveValue = curveValue;
		this.feature = feature;
	}

	@Override
	public BigInteger getCurveOrder() {
		return curveValue;
	}

	@Override
	public Object getFeature() {
		return feature;
	}

	@Override
	public int compareTo(ICurveOrder other) {
		return curveValue.compareTo(other.getCurveOrder());
	}

	@Override
	public FeaturesKeyClassified getFeatureKey() {
		return feature.toFeaturesKeyClassified();
	}
}
