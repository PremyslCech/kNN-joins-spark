package cz.siret.knn;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

public class SparkMetricsRegistrator {

	private static class KnnSparkListener extends SparkListener {

		private final KnnMetrics kNNmetrics = new KnnMetrics();

		@Override
		public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
			synchronized (kNNmetrics) {
				kNNmetrics.addShuffleBytesRead(taskEnd.taskMetrics().shuffleReadMetrics().totalBytesRead());
				kNNmetrics.addShuffleBytesWrite(taskEnd.taskMetrics().shuffleWriteMetrics().bytesWritten());
				kNNmetrics.addResultsSize(taskEnd.taskMetrics().resultSize());
				kNNmetrics.addPeakExecutionMemory(taskEnd.taskMetrics().peakExecutionMemory());
				kNNmetrics.addCpuTime(taskEnd.taskMetrics().executorCpuTime());
				kNNmetrics.addRunTime(taskEnd.taskMetrics().executorRunTime());
			}
		}

//		@Override
//		public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
//			synchronized (kNNmetrics) {
//				kNNmetrics.addShuffleBytesRead(stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBytesRead());
//				kNNmetrics.addShuffleBytesWrite(stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().bytesWritten());
//				kNNmetrics.addResultsSize(stageCompleted.stageInfo().taskMetrics().resultSize());
//				kNNmetrics.addPeakExecutionMemory(stageCompleted.stageInfo().taskMetrics().peakExecutionMemory());
//				kNNmetrics.addCpuTime(stageCompleted.stageInfo().taskMetrics().executorCpuTime());
//				kNNmetrics.addRunTime(stageCompleted.stageInfo().taskMetrics().executorRunTime());
//			}

//		}
	}

	public static KnnMetrics register(SparkContext sparkContext) {

		KnnSparkListener knnSparkListener = new KnnSparkListener();
		sparkContext.addSparkListener(knnSparkListener);

		return knnSparkListener.kNNmetrics;
	}

}
