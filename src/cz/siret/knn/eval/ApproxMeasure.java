package cz.siret.knn.eval;

public class ApproxMeasure {

	protected final String description;

	protected final float min;
	protected final float max;
	protected final float average;

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

	public String getOutput(String separator, long totalCount) {
		StringBuilder output = new StringBuilder();
		output.append(min).append(separator).append(max).append(separator).append(getAverage(totalCount));
		return output.toString();
	}

	private float getAverage(long count) {
		return average / count;
	}
}
