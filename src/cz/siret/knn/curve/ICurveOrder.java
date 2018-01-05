package cz.siret.knn.curve;

import java.io.Serializable;
import java.math.BigInteger;

import cz.siret.knn.model.FeaturesKeyClassified;

public interface ICurveOrder extends Comparable<ICurveOrder>, Serializable {

	BigInteger getCurveOrder();
	
	Object getFeature();
	
	FeaturesKeyClassified getFeatureKey();
	
}
