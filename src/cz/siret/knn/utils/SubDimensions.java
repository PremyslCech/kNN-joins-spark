package cz.siret.knn.utils;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cz.siret.knn.SparkUtils;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;
import cz.siret.knn.model.Pair;

/**
 * Gets sub dimensions from given features
 * 
 * @author pcech
 *
 */
public class SubDimensions {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		public void run(String inputPath, String outputPath, final int dimension) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Sub dimension extractor").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);

			SparkUtils sparkUtils = new SparkUtils();
			sparkUtils.deleteOutputDirectory(jsc.hadoopConfiguration(), outputPath);

			JavaRDD<Feature> reducedFeatures = sparkUtils.readInputPath(jsc, inputPath, 1).map(new Function<Feature, Feature>() {
				public Feature call(Feature origFeature) throws Exception {

					int[] featureKeys = origFeature.getKeys();
					ArrayList<Pair> outputPairs = new ArrayList<>();
					for (int i = 0; i < featureKeys.length && featureKeys[i] < dimension; i++) {
						outputPairs.add(new Pair(featureKeys[i], origFeature.getValues()[i]));
					}
					// last index must be dimension index
					if (outputPairs.isEmpty() || outputPairs.get(outputPairs.size() - 1).getKey() != dimension - 1) {
						outputPairs.add(new Pair(dimension - 1, origFeature.getOtherBinsValue()));
					}

					return new Feature(origFeature.getKey(), origFeature.getClassification(), origFeature.getOtherBinsValue(), outputPairs);
				}
			});

			reducedFeatures.saveAsTextFile(outputPath);
			System.out.println("Sub dimensions (" + dimension + ") done!");

			jsc.close();
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Wrong number of arguments, usage is: <input_path> <output_path> <desired_dimension>");
			return;
		}

		Job job = new Job();
		job.run(args[0], args[1], Integer.valueOf(args[2]));
	}
}
