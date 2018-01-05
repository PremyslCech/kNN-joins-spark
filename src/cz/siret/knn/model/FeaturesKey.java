package cz.siret.knn.model;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FeaturesKey implements Comparable<FeaturesKey>, Serializable {

	private int fiveMinuteId;
	private int clientId;
	private int serverId;

	public FeaturesKey() {
	}

	public FeaturesKey(int fiveMinuteId, int clientId, int serverId) {
		this.fiveMinuteId = fiveMinuteId;
		this.clientId = clientId;
		this.serverId = serverId;
	}

	public FeaturesKey(int fiveMinuteId, int clientId) {
		this(fiveMinuteId, clientId, 0);
	}

	public FeaturesKey(FeaturesKey fk) {
		fiveMinuteId = fk.getFiveMinuteId();
		clientId = fk.getClientId();
		serverId = fk.getServerId();
	}

	@Override
	public int compareTo(FeaturesKey other) {

		int fiveMinuteComparison = Integer.compare(fiveMinuteId, other.fiveMinuteId);
		int clientIdComparison = Integer.compare(clientId, other.clientId);
		int serverIdComparison = Integer.compare(serverId, other.serverId);

		return fiveMinuteComparison != 0 ? fiveMinuteComparison
				: clientIdComparison != 0 ? clientIdComparison : serverIdComparison;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = 1;
		result = prime * result + fiveMinuteId;
		result = prime * result + clientId;
		result = prime * result + serverId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof FeaturesKey)) {
			return false;
		}

		FeaturesKey other = (FeaturesKey) obj;
		return this.fiveMinuteId == other.fiveMinuteId && this.clientId == other.clientId
				&& this.serverId == other.serverId;
	}

	@Override
	public String toString() {

		return serverId == 0 ? String.format("%1$d_%2$d", fiveMinuteId, clientId)
				: String.format("%1$d_%2$d_%3$d", fiveMinuteId, clientId, serverId);
	}

	public static FeaturesKey parse(String line) {
		try {
			int fiveMinuteId = 0, serverId = 0, clientId;

			if (line.contains("_")) {
				String[] strId = line.split("_");
				fiveMinuteId = Integer.parseInt(strId[0]);
				clientId = Integer.parseInt(strId[1]);
				if (strId.length == 3) {
					serverId = Integer.parseInt(strId[2]);
				}
			} else {
				clientId = Integer.parseInt(line);
			}
			return new FeaturesKey(fiveMinuteId, clientId, serverId);
		} catch (Exception e) {
			throw new RuntimeException("Could not parse line '" + line + "'");
		}
	}

	public FeaturesKey clone() {
		return new FeaturesKey(fiveMinuteId, clientId, serverId);
	}

	public int getFiveMinuteId() {
		return fiveMinuteId;
	}

	public int getClientId() {
		return clientId;
	}

	public int getServerId() {
		return serverId;
	}

}
