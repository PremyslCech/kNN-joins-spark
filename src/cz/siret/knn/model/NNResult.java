package cz.siret.knn.model;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("serial")
public class NNResult implements Serializable {

	public FeaturesKey queryKey = new FeaturesKeyClassified();
	public int queryClassification = 0;
	public List<FeaturesKey> dbKeys = new ArrayList<>();
	public List<Float> dbDistances = new ArrayList<>();
	public List<Integer> dbClassifications = new ArrayList<>();

	public static NNResult parse(String line) throws IOException {
		try {
			String[] entries = line.split("\t");
			NNResult res = new NNResult();

			// first entry is for query
			String[] queryArr = entries[0].split(",");
			if (queryArr.length != 2)
				throw new IOException();
			res.queryKey = FeaturesKey.parse(queryArr[0]);
			res.queryClassification = Integer.valueOf(queryArr[1]);

			List<Neighbor> neighbors = new ArrayList<>();

			// others for db
			for (int i = 1; i < entries.length; i++) {
				String[] dbArr = entries[i].split("\\|");
				if (dbArr.length != 3)
					throw new IOException();
				FeaturesKey dbKey = FeaturesKey.parse(dbArr[0]);
				float dbDist = Float.valueOf(dbArr[1]);
				int dbClassification = Integer.valueOf(dbArr[2]);

				neighbors.add(new Neighbor(dbKey, dbDist, dbClassification));
			}

			Collections.sort(neighbors);
			for (Neighbor n : neighbors) {
				res.dbKeys.add(n.key);
				res.dbDistances.add(n.distance);
				res.dbClassifications.add(n.classification);
			}

			return res;
		} catch (Exception e) {
			throw new IOException("Cannot parse NNResult, line '" + line + "'", e);
		}
	}

	@Override
	public String toString() {
		String queryStr = queryKey + "," + queryClassification;
		StringBuilder dbStr = new StringBuilder();
		for (int i = 0; i < dbKeys.size(); i++) {
			dbStr.append(dbKeys.get(i)).append("|").append(dbDistances.get(i));
			dbStr.append("|").append(dbClassifications.get(i));
			if (i < dbKeys.size() - 1)
				dbStr.append("\t");
		}
		return queryStr + "\t" + dbStr;
	}

	private static class Neighbor implements Comparable<Neighbor> {
		public FeaturesKey key;
		public Float distance;
		public Integer classification;

		public Neighbor(FeaturesKey key, float distance, int classification) {
			this.key = key;
			this.distance = distance;
			this.classification = classification;
		}

		@Override
		public int compareTo(Neighbor other) {
			int compare;
			if ((compare = distance.compareTo(other.distance)) != 0) {
				return compare;
			}
			return key.compareTo(other.key);
		}
	}

	public static void main(String[] args) throws IOException {
		BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
		NNResult nn = NNResult.parse(r.readLine());
		System.out.println(nn);
	}
}
