package cz.siret.knn.pivot;

import java.io.Serializable;

@SuppressWarnings("serial")
public class VoronoiStatistics implements Serializable {

	private PartitionStatistics[] databaseStatistics;
	private PartitionStatistics[] queryStatistics;

	public VoronoiStatistics(PartitionStatistics[] databaseStatistics, PartitionStatistics[] queryStatistics) {
		this.databaseStatistics = databaseStatistics;
		this.queryStatistics = queryStatistics;
	}

	public PartitionStatistics[] get(boolean isDatabase) {
		return isDatabase ? databaseStatistics : queryStatistics;
	}

	public PartitionStatistics[] getDatabaseStatistics() {
		return databaseStatistics;
	}

	public PartitionStatistics[] getQueryStatistics() {
		return queryStatistics;
	}
}
