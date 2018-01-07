package cz.siret.knn;

public class KnnMetrics {

	private long shuffleBytesRead = 0;
	private long shuffleBytesWrite = 0;
	private long resultsSize = 0;
	private long peakExecutionMemory = 0;
	private long totalCpuTime = 0;
	private long totalRunTime = 0;

	public String produceOutput() {
		StringBuilder output = new StringBuilder();
		output.append("======== Knn Metrics ========").append(System.lineSeparator());
		output.append("Shuffle read (MB): ").append(shuffleBytesRead / (1024 * 1024d)).append(System.lineSeparator()); // in MBs
		output.append("Shuffle write (MB): ").append(shuffleBytesWrite / (1024 * 1024d)).append(System.lineSeparator()); // in MBs
		output.append("Results size (MB): ").append(resultsSize / (1024 * 1024d)).append(System.lineSeparator()); // in MBs
		output.append("Peak execution memory (MB): ").append(peakExecutionMemory / (1024 * 1024d)).append(System.lineSeparator()); // in MBs
		output.append("Total CPU time (s): ").append(totalCpuTime / Math.pow(10, 9)).append(System.lineSeparator()); // in seconds
		output.append("Total Running time (s): ").append(totalRunTime / 1000d); // in seconds
		return output.toString();
	}

	public void addShuffleBytesRead(long value) {
		shuffleBytesRead += value;
	}

	public void addShuffleBytesWrite(long value) {
		shuffleBytesWrite += value;
	}

	public void addResultsSize(long value) {
		resultsSize += value;
	}

	public void addPeakExecutionMemory(long value) {
		peakExecutionMemory = Math.max(peakExecutionMemory, value);
	}

	public void addCpuTime(long value) {
		totalCpuTime += value;
	}

	public void addRunTime(long value) {
		totalRunTime += value;
	}
}
