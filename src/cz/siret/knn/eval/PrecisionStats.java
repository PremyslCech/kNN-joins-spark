package cz.siret.knn.eval;

public class PrecisionStats {

	private final float precision;
	private final float distance;
	private final float distRatio;
	private final float effectiveError;
	private final float avgEffectiveError;
	private final long count;

	public PrecisionStats(float precision, float distance, float distRatio, float effectiveError, float avgEffectiveError, long count) {
		this.precision = precision;
		this.distance = distance;
		this.distRatio = distRatio;
		this.effectiveError = effectiveError;
		this.avgEffectiveError = avgEffectiveError;
		this.count = count;

	}

	public float getPrecision() {
		return precision;
	}

	public float getDistance() {
		return distance;
	}

	public float getDistRatio() {
		return distRatio;
	}

	public float getEffectiveError() {
		return effectiveError;
	}

	public float getAvgEffectiveError() {
		return avgEffectiveError;
	}
	
	public long getCount() {
		return count;
	}
}
