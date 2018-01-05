package cz.siret.knn;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cz.siret.knn.curve.PartitionBoundaries;
import cz.siret.knn.curve.Partitions;
import cz.siret.knn.lsh.DimensionStats;
import cz.siret.knn.lsh.HashTable;
import cz.siret.knn.model.FeaturesKey;
import cz.siret.knn.pivot.PartitionStatistics;
import cz.siret.knn.pivot.VoronoiStatistics;

public class Logger {

	private final boolean enableLogging;

	public Logger(boolean enableLogging) {
		this.enableLogging = enableLogging;

		File logDir = new File("log");
		if (!logDir.exists()) {
			logDir.mkdir();
		}
	}

	public void logVoronoiStats(VoronoiStatistics voronoiStatistics) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/pivotVoronoiStats")) {
			for (PartitionStatistics stats : voronoiStatistics.getDatabaseStatistics()) {
				writer.write(stats.getMinDist() + " " + stats.getMaxDist() + " " + stats.getNumberOfObjects() + " " + stats.getSizeOfObjects() + " ");
				for (Float val : stats.getDistancesToNearestObjects()) {
					writer.write(val + " ");
				}
				for (int i = 0; i < stats.getCutRegion().getMinDistances().length; i++) {
					writer.write(stats.getCutRegion().getMinDistances()[i] + "|" + stats.getCutRegion().getMaxDistances()[i] + " ");
				}
				writer.write(System.lineSeparator());
			}
			writer.write("---------------------------------------------");
			writer.write(System.lineSeparator());

			for (PartitionStatistics stats : voronoiStatistics.getQueryStatistics()) {
				writer.write(stats.getMinDist() + " " + stats.getMaxDist() + " " + stats.getNumberOfObjects() + " " + stats.getSizeOfObjects() + " ");
				if (stats.getDistancesToNearestObjects() != null) {
					for (Float val : stats.getDistancesToNearestObjects()) {
						writer.write(val + " ");
					}
				}
				if (stats.getCutRegion() != null) {
					for (int i = 0; i < stats.getCutRegion().getMinDistances().length; i++) {
						writer.write(stats.getCutRegion().getMinDistances()[i] + "|" + stats.getCutRegion().getMaxDistances()[i] + " ");
					}
				}
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logLowerBounds(List<List<ObjectWithDistance>> lbOfPartitionSToGroups) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/pivotLowerBounds")) {
			for (List<ObjectWithDistance> lowerBounds : lbOfPartitionSToGroups) {
				for (ObjectWithDistance lowerBound : lowerBounds) {
					writer.write(lowerBound.getObjectId() + ":" + lowerBound.getDistance() + " ");
				}
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logGroups(int[] groups, int groupsCount) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/pivotGroups")) {
			for (int i = 0; i < groupsCount; i++) {
				for (int pivodId = 0; pivodId < groups.length; pivodId++) {
					int groupId = groups[pivodId];
					if (groupId == i) {
						writer.write(pivodId + " ");
					}
				}
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logPivotDistances(float[][] pivotDistances) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/pivotDistances")) {
			for (int i = 0; i < pivotDistances.length; i++) {
				for (int j = 0; j < pivotDistances[i].length; j++) {
					writer.write(pivotDistances[i][j] + " ");
				}
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logUpperBoundsForAllQueries(Map<FeaturesKey, Float> ubForQueries, int k, int maxRecDepth, float filterRatio, int pivotCount,
			int numberOfGroups) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/pivotUbQueries")) {
			for (Entry<FeaturesKey, Float> ub : ubForQueries.entrySet()) {

				writer.write(ub.getKey() + ";" + ub.getValue() + ";UpperBound;" + k + ";" + maxRecDepth + ";" + filterRatio + ";" + pivotCount + ";" + numberOfGroups);
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logCurvePartitions(Partitions partitions) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/curvePartitions")) {
			writer.write("-----------------------------------DB Boundaries--------------------------------------");
			writer.write(System.lineSeparator());
			for (List<PartitionBoundaries> dbBoundaries : partitions.getDbBoundaries()) {
				for (PartitionBoundaries partitionBoundaries : dbBoundaries) {
					writer.write(partitionBoundaries.getStart() + ";" + partitionBoundaries.getEnd() + " ");
				}
				writer.write(System.lineSeparator());
			}
			writer.write("-----------------------------------Query Boundaries--------------------------------------");
			writer.write(System.lineSeparator());
			for (List<PartitionBoundaries> dbBoundaries : partitions.getQueryBoundaries()) {
				for (PartitionBoundaries partitionBoundaries : dbBoundaries) {
					writer.write(partitionBoundaries.getStart() + ";" + partitionBoundaries.getEnd() + " ");
				}
				writer.write(System.lineSeparator());
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void logDimensionStats(DimensionStats[] dimensionStats) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/lshDimensionStats")) {

			for (DimensionStats dimStats : dimensionStats) {
				writer.write(dimStats.getExpectedValue() + " " + dimStats.getStandardDeviation() + System.lineSeparator());
			}

			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void logHashTables(HashTable[] hashTables) {

		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/lshHashTables")) {

			for (HashTable hashTable : hashTables) {
				writer.write(hashTable.toString());
				writer.write(System.lineSeparator());
			}

			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void logStaticPivots(int[] staticPivotIds) {
		
		if (!enableLogging) {
			return;
		}

		try (FileWriter writer = new FileWriter("log/staticPivots")) {
			for (int pivotId : staticPivotIds) {
				writer.write(pivotId + " ");
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
