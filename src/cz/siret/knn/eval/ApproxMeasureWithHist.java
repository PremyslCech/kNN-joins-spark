package cz.siret.knn.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ApproxMeasureWithHist extends ApproxMeasure {

	private List<Float> values;
	private static final int MAX_HIST_BINS = 500;

	public ApproxMeasureWithHist(String description, float value) {
		this(description, value, value, value, Arrays.asList(value));
	}

	public ApproxMeasureWithHist(String description, float min, float max, float average, List<Float> values) {
		super(description, min, max, average);

		this.values = values;
	}

	@Override
	public ApproxMeasure combine(ApproxMeasure other) {

		if (!description.equals(other.description)) {
			throw new IllegalArgumentException("Uncompatible approximation measure to combine. (" + description + "," + other.description + ")");
		}

		List<Float> newValues = new ArrayList<>();
		newValues.addAll(values);
		newValues.addAll(((ApproxMeasureWithHist) other).values);
		return new ApproxMeasureWithHist(description, Math.min(min, other.min), Math.max(max, other.max), average + other.average, newValues);
	}

	@Override
	public String getOutput(String separator, long totalCount) {

		return super.getOutput(separator, totalCount) + separator + getHistogram();
	}

	private String getHistogram() {

		final char binSeparator = ';';
		final char valSeparator = ':';

		float step = (max - min) / MAX_HIST_BINS;
		int[] hist = new int[MAX_HIST_BINS + 1];

		for (float val : values) {
			int binId = (int) Math.floor((val - min) / step);
			++hist[binId];
		}

		StringBuilder output = new StringBuilder();
		for (int i = 0; i < hist.length; i++) {

			output.append(min + i * step).append(valSeparator).append(hist[i]);
			if (i < hist.length - 1) {
				output.append(binSeparator);
			}
		}
		return output.toString();
	}

}
