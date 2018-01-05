package cz.siret.knn.pivot;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;

import cz.siret.knn.metric.IMetric;

/**
 * Let N be the number of groups to be generated
 * 
 * We organize partitions (pivots) into a number of groups as follows: (1) select the first pivot p1 that takes the farthest distance to the rest of pivots and
 * add it to the first group (2) select the second pivot that takes the farthest distance to p1 and add it to the second group (3) for (i = 3 to N): select the
 * pivot that takes the farthest distance to the selected pivots and add it to the ith group
 * 
 * After assigning the first partition for each group, we do the following iteration until all partitions are assigned to the groups: At each iteration: (1)
 * select the group with the minimum size of objects (2) select the pivot pi that takes the minimum distance to the pivots in this group and assign it to the
 * group
 * 
 */
public class GeometricGroupingBySize {
	private Vector<Object> pivots;
	private Vector<Integer> numOfObjectsInPivot;
	private Vector<Long> sizeOfObjectsInPivot;
	private Vector<Integer>[] pivotsInGroup;
	private long[] numOfObjectsInGroup;
	private long[] sizeOfObjectsInGroup;
	int totalNumOfObjects = 0;
	int numOfPivots;
	int numOfGroups;
	private IMetric metric = null;
	Configuration conf;

	@SuppressWarnings("unchecked")
	public GeometricGroupingBySize(IMetric metric, List<Object> pivots, List<Long> sizeOfObjectsInPivot, List<Integer> numOfObjectsInPivot, int numberOfGroups)
			throws IOException {
		this.metric = metric;
		this.pivots = new Vector<>(pivots);
		this.numOfObjectsInPivot = new Vector<>(numOfObjectsInPivot);
		this.sizeOfObjectsInPivot = new Vector<>(sizeOfObjectsInPivot);
		// organize pivots into groups
		numOfGroups = numberOfGroups;
		numOfPivots = pivots.size();
		// set the pivots for each group
		pivotsInGroup = new Vector[numOfGroups];
		// set the number of objects for each group
		numOfObjectsInGroup = new long[numOfGroups];
		sizeOfObjectsInGroup = new long[numOfGroups];
	}

	public Vector<Integer>[] doGouping() throws IOException {
		if (numOfGroups > numOfPivots) {
			throw new IOException("The number of groups (" + numOfGroups + ") exceeds the number of pivots (" + numOfPivots + ")!");
		}

		int i, j;
		int pid, gid, id, tmpPid;
		/** maintain the rest of pivots that have not been assigned */
		Vector<Integer> restOfPivots = new Vector<Integer>(numOfPivots);

		/** initialize the first partition for each group */
		int[] initSetOfParts = initPartsForGroups(restOfPivots);

		/**
		 * maintain the sum of distance of each pivot to the pivots of the groups
		 */
		float[][] distMatrix = new float[numOfGroups][numOfPivots];

		for (i = 0; i < numOfGroups; i++) {
			pivotsInGroup[i] = new Vector<Integer>();
			/** add the pid to the group */
			pid = initSetOfParts[i];
			pivotsInGroup[i].add(pid);
			/** set size to the number of objects in the partition */
			numOfObjectsInGroup[i] = numOfObjectsInPivot.get(pid);
			sizeOfObjectsInGroup[i] = sizeOfObjectsInPivot.get(pid);
			/**
			 * compute the distance of the remaining pivots to the select pivot
			 */
			for (j = 0; j < numOfPivots; j++) {
				distMatrix[i][j] = metric.dist(pivots.get(j), pivots.get(pid));
			}
		}

		/** set the position of assigned pivots to be max */
		for (i = 0; i < numOfGroups; i++) {
			for (j = 0; j < numOfGroups; j++) {
				pid = pivotsInGroup[j].get(0);
				distMatrix[i][pid] = Float.MAX_VALUE;
			}
		}

		// start the iteration
		while (restOfPivots.size() > 0) {
			// select
			gid = selMinSizeOfGroup(sizeOfObjectsInGroup);
			id = findNearestPivot(distMatrix[gid], restOfPivots);
			pid = restOfPivots.get(id);
			// update
			restOfPivots.remove(id);
			pivotsInGroup[gid].add(pid);
			numOfObjectsInGroup[gid] += numOfObjectsInPivot.get(pid);
			sizeOfObjectsInGroup[gid] += sizeOfObjectsInPivot.get(pid);

			if (restOfPivots.size() == 0)
				break;

			// set the pivot pid to be selected
			for (i = 0; i < numOfGroups; i++) {
				distMatrix[i][pid] = Float.MAX_VALUE;
			}
			// update the distance of pivots in restOfPivots to group gid
			for (i = 0; i < restOfPivots.size(); i++) {
				tmpPid = restOfPivots.get(i);
				distMatrix[gid][tmpPid] += metric.dist(pivots.get(tmpPid), pivots.get(pid));
			}
		}

		return pivotsInGroup;
	}

	private int[] initPartsForGroups(Vector<Integer> pids) throws IOException {
		int i, j, id;
		float dist, sumOfDist;
		int[] initSetOfParts = new int[numOfGroups];
		Vector<Float> distToSelPivots = new Vector<Float>(numOfPivots);

		for (i = 0; i < numOfPivots; i++)
			pids.add(i);

		/** select the first pid */
		id = selectFirstPId();
		initSetOfParts[0] = pids.get(id);
		/** compute the distance of pivots to pid */
		for (i = 0; i < numOfPivots; i++) {
			dist = metric.dist(pivots.get(i), pivots.get(id));
			distToSelPivots.add(dist);
		}
		/** remove the id */
		pids.remove(id);
		distToSelPivots.remove(id);
		for (i = 1; i < numOfGroups; i++) {
			id = selectNextPId(distToSelPivots);
			initSetOfParts[i] = pids.get(id);
			/** remove the id */
			pids.remove(id);
			distToSelPivots.remove(id);
			// update the distance of the unselected pivots
			for (j = 0; j < pids.size() && i < numOfGroups - 1; j++) {
				sumOfDist = distToSelPivots.get(j);
				dist = metric.dist(pivots.get(initSetOfParts[i]), pivots.get(pids.get(j)));
				distToSelPivots.set(j, sumOfDist + dist);
			}
		}

		return initSetOfParts;
	}

	private int selMinSizeOfGroup(long[] sizeOfGroups) throws IOException {
		if (sizeOfGroups == null || sizeOfGroups.length == 0)
			throw new IOException("the input array cannot be null or empty!");

		int id = 0;
		long minSize = Long.MAX_VALUE;

		for (int i = 0; i < sizeOfGroups.length; i++) {
			if (sizeOfGroups[i] < minSize) {
				minSize = sizeOfGroups[i];
				id = i;
			}
		}

		return id;
	}

	private int findNearestPivot(float[] distMatrix, Vector<Integer> restOfPids) throws IOException {
		if (distMatrix == null || distMatrix.length != numOfPivots) {
			throw new IOException("An error in function findNearestPivot!");
		}

		float minDist = Float.MAX_VALUE;
		int id = -1, tmpPid;

		for (int i = 0; i < restOfPids.size(); i++) {
			tmpPid = restOfPids.get(i);
			if (distMatrix[tmpPid] == Float.MAX_VALUE) {
				throw new IOException("distMatrix[" + tmpPid + "] cannot be the maximum of float");
			}
			if (distMatrix[tmpPid] < minDist) {
				minDist = distMatrix[tmpPid];
				id = i;
			}
		}

		return id;
	}

	/**
	 * select the pivots that are furthest to the rest of pivots
	 * 
	 * @return
	 * @throws IOException
	 */
	private int selectFirstPId() throws IOException {
		int maxSumOfPID = 0;
		float sumOfDist, maxSumOfDist = 0;
		float dist;

		for (int i = 0; i < numOfPivots; i++) {
			sumOfDist = 0.0f;
			for (int j = 0; j < numOfPivots; j++) {
				if (i == j)
					dist = 0.0f;
				else
					dist = metric.dist(pivots.get(i), pivots.get(j));

				sumOfDist += dist;
			}
			if (sumOfDist > maxSumOfDist) {
				maxSumOfPID = i;
				maxSumOfDist = sumOfDist;
			}
		}

		return maxSumOfPID;
	}

	/**
	 * select the pivot with the furthest distance to the selected pivots
	 * 
	 * @param sumOfDist
	 * @return the id (not the pid) in pids that indicates pids.get(id) with the furthest distance
	 * @throws IOException
	 */
	private int selectNextPId(Vector<Float> distToSelPivots) throws IOException {
		int maxOfID = 0;
		float dist, maxDist = 0;

		assert (distToSelPivots != null);
		for (int i = 0; i < distToSelPivots.size(); i++) {
			dist = distToSelPivots.get(i);
			if (maxDist < dist) {
				maxDist = dist;
				maxOfID = i;
			}
		}

		return maxOfID;
	}

}
