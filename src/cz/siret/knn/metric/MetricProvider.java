package cz.siret.knn.metric;

public class MetricProvider {

	private static IMetric metric = null;

	public static IMetric getMetric() {
		if (metric == null) {
			metric = initMetric();
		}
		return metric;
	}

	private static IMetric initMetric() {
		return new L2MetricSiret();
	}

}
