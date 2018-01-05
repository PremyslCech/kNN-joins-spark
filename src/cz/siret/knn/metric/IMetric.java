package cz.siret.knn.metric;

import java.io.IOException;
import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class IMetric implements Serializable {

	public long numOfDistComp = 0;

	public abstract float dist(Object o1, Object o2) throws IOException;

	public abstract long getNumOfDistComp();
}
