package cz.siret.knn.pivot;

import java.io.Serializable;

import cz.siret.knn.model.Feature;

public interface IFeatureWithPartition extends Serializable {

	Feature getFeature();
	
	int getNearestPivot();

	int[] getNearestPivots();

	boolean isDatabase();

	float getDistanceToPivot();
	
	float[] getDistancesToStaticPivots();

}