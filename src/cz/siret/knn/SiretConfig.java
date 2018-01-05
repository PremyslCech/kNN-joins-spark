package cz.siret.knn;

public class SiretConfig {

	// global parameters
	public static final String REDUCERS_COUNT_STR = "spark.siret.knnjoin.reducers.count";
	public static final String MIN_PARITTIONS_COUNT_STR = "spark.siret.knnjoin.min.partitions.count";
	public static final String K_STR = "spark.siret.knnjoin.k";
	
	public static final String FIELD_SEPARATOR_STR = ";";

	// parameters for the pivot method
	public static final String PIVOT_COUNT_STR = "spark.siret.knnjoin.pivot.count";
	public static final String STATIC_PIVOT_COUNT_STR = "spark.siret.knnjoin.pivot.static.count";	//if 0 CR are not used
	public static final String MAX_SPLIT_DEPTH_STR = "spark.siret.knnjoin.pivot.split.depth";
	public static final String FILTER_STR = "spark.siret.knnjoin.pivot.filter";
	public static final String EPSILON_STR = "spark.siret.knnjoin.pivot.epsilon";
	public static final String EPSILON_MULTIPLIER_STR = "spark.siret.knnjoin.pivot.epsilon.multiplier";
	public static final String PIVOT_METHOD_STR = "spark.siret.knnjoin.pivot.method";

	// parameters for the stats calculator
	public static final String METHODS_STR = "spark.siret.knnjoin.pivot.stats.methods";
	public static final String KVALUES_STR = "spark.siret.knnjoin.pivot.stats.kvalues";
	public static final String PIVOTCOUNTS_STR = "spark.siret.knnjoin.pivot.stats.pivotCounts";
	public static final String MAX_REC_DEPTHS_STR = "spark.siret.knnjoin.pivot.stats.maxRecDepths";
	public static final String MAX_REDUCERS_STR = "spark.siret.knnjoin.pivot.stats.maxReducers";
	public static final String FILTER_RATIOS_STR = "spark.siret.knnjoin.pivot.stats.filterRatios";
	
	// parameters for the curve join
	public static final String SHIFTS_STR = "spark.siret.knnjoin.curve.shifts";
	public static final String CURVE_RANGE_STR = "spark.siret.knnjoin.curve.range";
	public static final String CURVE_SCALE_STR = "spark.siret.knnjoin.curve.scale";
	public static final String SAMPLING_RATE_STR = "spark.siret.knnjoin.curve.samplingRate";
	public static final String CONSIDER_ENTIRE_BUCKETS_STR = "spark.siret.knnjoin.curve.entireBuckets";
	public static final String ONLY_Z_ORDER_STR = "spark.siret.knnjoin.curve.onlyZorder";
	
	// parameters for the LSH
	public static final String HASH_TABLES_STR = "spark.siret.knnjoin.lsh.hashTables";
	public static final String HASH_FUNCTIONS_STR = "spark.siret.knnjoin.lsh.hashFunctions";
	public static final String W_STR = "spark.siret.knnjoin.lsh.w";
	public static final String HASH_FUNC_MULTIPLIER_STR = "spark.siret.knnjoin.hashFunctionsGeneratorMultiplier";
}
