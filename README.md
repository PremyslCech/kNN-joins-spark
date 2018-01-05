# kNN-joins-spark
MapReduce based similarity k-NN joins running on Spark.
Our solution is implemented in Java 1.7 to allow back compatibility with older systems.
MapReduce algorithms were tested on Spark 2.2.0 using EMR Amazon clusters.



#### Building and running jobs through Ant ####

External Lib dependecies (example):

	<classpathentry kind="lib" path="ant/lib/spark-core_2.11-2.2.0.jar"/>
	<classpathentry kind="lib" path="ant/lib/scala-library-2.11.8.jar"/>
	<classpathentry kind="lib" path="ant/lib/spark-mllib_2.11-2.1.1.jar"/>
	<classpathentry kind="lib" path="ant/lib/hadoop-common-2.5.1.jar"/>
	
Then run build.xml using ANT.

### Setup ###

Configurations are provided in the conf file. Example is in the "siret.conf" file. Usage: --properties-file siret.conf




# Execution #

Input format of both query (R) and database (S) data (sparse text format):

	Fields are separated by semicolon ";".
	First field: 				ID - have two parts <id1>_<id2> (two integers)
	Second field: 				Classification (integer)
	Third field:				Not specified bins values (float)
	Then follow pairs of specified bins: 	<bin_ID>:<value> (integer : float)
	
	Important note! The last dimension bin must be present (so the algorithms can automatically determine histogram dimensionality).
	
	Example 1:
	1_1;0;0;0:0.2;1:0.8;9:0
	
	Histogram with id: 1_1; classification = 0; common value = 0 (bins 2,3,4,5,6,7,8 = 0) and dimensionality = 10.
	Observe that the last bin is present even though the 9 bin value = 0 (= common value - third field).
	
	Example 2:
	12_69924;1;0.0016611295;6:0.0154;7:0.0016;8:0.87;16:0.11;...;199:0.12
	
	For not specified histogram bins (e.g. 0, 1, 2, 3, 4, 5, 9, 10, ...) the value 0.0016611295 is used.
	This histogram has 200 dimensions (the last dimension 199 must be present even if it is equal to the common value).
	
	
	
k-NN joins output format:

	<query_id>,<query_classification>\t<neighbor_1_id>|<neighbor_1_classification>|<neighbor_1_distance>\t<neighbor_2_id>|<neighbor_2_classification>|<neighbor_2_distance>\t...\t<neighbor_k_id>|<neighbor_k_classification>|<neighbor_k_distance>
	
	For example (k = 5):
	2_1,0	1_10|0|0.1	2_7|0|0.15	3_3|0|0.19	3_10|0|0.28	1_18|1|0.5
	
	Neighbors should be sorted in an ascending order.


## Pivot based kNN join (approx)

spark-submit --class cz.siret.knn.pivot.KNNJoinApprox --properties-file siret.conf --conf spark.siret.knnjoin.k=10 --executor-memory 4500m --master yarn --deploy-mode client \
	--num-executors 19 Siret.jar <database_path> <queries_path> <output_path>
	
Different possible variants (enable through spark.siret.knnjoin.pivot.method):

	EXACT - exact k-NN join
	APPROX - heuristic approximate k-NN join
	EPSILON_APPROX - epsilon guaranted approximate k-NN join
	
	EXACT_BOUNDS - computes upper bounds for the EXACT variant
	APPROX_BOUNDS - computes upper bounds for the APPROX variant - can be used to predict parameters performance (see our paper)

	
## Naive exact kNN join 

spark-submit --class cz.siret.knn.naive.NaiveKNNJoin --executor-memory 4500m --driver-memory 4500m --master yarn --deploy-mode client \
	--num-executors 19 Siret.jar <database_path> <queries_path> <output_path> <k> <numberOfPartitions>
	
## Curve (Z-curve)

spark-submit --class cz.siret.knn.curve.KNNJoinApprox --properties-file siret.conf --master yarn --deploy-mode client \
	--num-executors 20 Siret.jar <database_path> <queries_path> <output_path>
	
## LSH

spark-submit --class cz.siret.knn.lsh.KNNJoinApprox --properties-file siret.conf --master yarn --deploy-mode client \
	--num-executors 20 Siret.jar <database_path> <queries_path> <output_path>

## Approximation evaluation
Computes different approximation measures compared to the exact k-NN join.

spark-submit --class cz.siret.knn.eval.PrecisionEvaluator --properties-file siret.conf --deploy-mode client --num-executors 20 Siret.jar <base_kNN> \<operator> <compare_kNN>

where \<operator> can be: "le" (less or equal), "ge" (greater or equal), "eq" (equal). The operator determines relation between sets of query object in <base_kNN> and <compare_kNN> results.

Output is provided in a table format with 6 columns:

	1. K
	2. average absolute distance difference to the k-th neighbor
	3. average approximation precision (recall) - average number of matching neighbors / K
	4. average distance ratio - the ratio of sum of distances of base and compare k-NN results
	5. average effective error (real epsilon of the approximation) - only to the k-th neighbor
	6. average total average effective error - for all neighbors from first to k-th


## K-Means
Simple usage of the Spark MLib.

spark-submit --class cz.siret.kmeans.KMeansExtended --executor-memory 1500m --master yarn --deploy-mode client --num-executors 55 Siret.jar <data_to_cluster> <output_clusters> <cluster_count> <iterations_count>


	
