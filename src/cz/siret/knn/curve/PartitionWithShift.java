package cz.siret.knn.curve;

public class PartitionWithShift {

	private final int shift;
	private final int partition;

	public PartitionWithShift(int partition, int shift) {
		this.partition = partition;
		this.shift = shift;
	}

	public int getPartition() {
		return partition;
	}

	public int getShift() {
		return shift;
	}

	@Override
	public int hashCode() {
		return shift * 1000000 + partition;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof PartitionWithShift)) {
			return false;
		}
		PartitionWithShift other = (PartitionWithShift) obj;
		return shift == other.shift && partition == other.partition;
	}
}
