package cz.siret.knn.curve;

import java.util.List;

public class Partitions {

	private final List<List<PartitionBoundaries>> queryBoundaries;
	private final List<List<PartitionBoundaries>> dbBoundaries;

	public Partitions(List<List<PartitionBoundaries>> queryBoundaries, List<List<PartitionBoundaries>> dbBoundaries) {
		this.queryBoundaries = queryBoundaries;
		this.dbBoundaries = dbBoundaries;
	}

	public List<List<PartitionBoundaries>> getQueryBoundaries() {
		return queryBoundaries;
	}

	public List<List<PartitionBoundaries>> getDbBoundaries() {
		return dbBoundaries;
	}

}
