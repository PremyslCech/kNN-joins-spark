package cz.siret.knn.curve;

import java.math.BigInteger;

public class PartitionBoundaries {

	private final BigInteger start;
	private final BigInteger end;

	public PartitionBoundaries(BigInteger start, BigInteger end) {
		this.start = start;
		this.end = end;
	}

	public BigInteger getStart() {
		return start;
	}
	
	public BigInteger getEnd() {
		return end;
	}	
}
