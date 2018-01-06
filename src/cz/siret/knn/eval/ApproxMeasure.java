package cz.siret.knn.eval;

public class ApproxMeasure {

	private final String description;

	private final float min;
	private final float max;
	private final float average;

	// initialization with only one value
	public ApproxMeasure(String description, float value) {
		this(description, value, value, value);
	}

	public ApproxMeasure(String description, float min, float max, float average) {
		this.min = min;
		this.max = max;
		this.average = average;
		this.description = description;
	}

	public ApproxMeasure combine(ApproxMeasure other) {
		if (!description.equals(other.description)) {
			throw new IllegalArgumentException("Uncompatible approximation measure to combine. (" + description + "," + other.description + ")");
		}

		return new ApproxMeasure(description, Math.min(min, other.min), Math.max(max, other.max), average + other.average);
	}

	public float getMin() {
		return min;
	}

	public float getMax() {
		return max;
	}

	public float getAverage(long count) {
		return average / count;
	}

	public String getDescription() {
		return description;
	}
}
