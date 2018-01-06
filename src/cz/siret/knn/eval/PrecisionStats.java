package cz.siret.knn.eval;

import java.util.ArrayList;
import java.util.List;

public class PrecisionStats {

	private final List<ApproxMeasure> approxMeasures;
	private final long count;

	public PrecisionStats(List<ApproxMeasure> approxMeasures, long count) {
		this.approxMeasures = approxMeasures;
		this.count = count;
	}

	public PrecisionStats combine(PrecisionStats other) {
		if (approxMeasures.size() != other.approxMeasures.size()) {
			throw new IllegalArgumentException("Different number of measures.");
		}

		List<ApproxMeasure> combinedMeasures = new ArrayList<ApproxMeasure>(approxMeasures.size());
		for (int i = 0; i < approxMeasures.size(); i++) {
			combinedMeasures.add(approxMeasures.get(i).combine(other.approxMeasures.get(i)));
		}

		return new PrecisionStats(combinedMeasures, count + other.count);
	}

	public String getOutput(long totalCount) {

		final String separator = "\t";

		StringBuilder output = new StringBuilder();
		for (int i = 0; i < approxMeasures.size(); i++) {

			ApproxMeasure approxMeasure = approxMeasures.get(i);
			output.append(approxMeasure.getMin()).append(separator).append(approxMeasure.getMax()).append(separator).append(approxMeasure.getAverage(totalCount));

			if (i < approxMeasures.size() - 1) {
				output.append(separator);
			}
		}
		return output.toString();
	}

	public long getCount() {
		return count;
	}
}
