package cz.siret.knn.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import cz.siret.knn.SparkUtils;
import cz.siret.knn.model.Feature;
import cz.siret.knn.model.FeaturesKey;

/**
 * Gets some data statistics
 * 
 * @author pcech
 *
 */
public class MinMax {

	@SuppressWarnings("serial")
	private static class Job implements Serializable {

		public void run(String inputPath) throws Exception {

			SparkConf conf = new SparkConf().setAppName("Min max value").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.registerKryoClasses(new Class[] { Feature.class, FeaturesKey.class });
			JavaSparkContext jsc = new JavaSparkContext(conf);

			SparkUtils sparkUtils = new SparkUtils();

			JavaRDD<Feature> features = sparkUtils.readInputPath(jsc, inputPath, 1);
			StatsResults statsResults = getMinMax(features);

			System.out.println("Min: " + statsResults.min);
			System.out.println("Max: " + statsResults.max);
			System.out.println("Min max application done!");

			jsc.close();
		}

		private StatsResults getMinMax(JavaRDD<Feature> features) {

			return features.flatMap(new FlatMapFunction<Feature, StatsResults>() {
				public Iterator<StatsResults> call(Feature feature) throws Exception {

					List<StatsResults> output = new ArrayList<>();
					for (float value : feature.getValues()) {
						output.add(new StatsResults(value, value));
					}
					output.add(new StatsResults(feature.getOtherBinsValue(), feature.getOtherBinsValue()));
					return output.iterator();
				}
			}).reduce(new Function2<MinMax.Job.StatsResults, MinMax.Job.StatsResults, MinMax.Job.StatsResults>() {
				public StatsResults call(StatsResults stats1, StatsResults stats2) throws Exception {

					return new StatsResults(Math.min(stats1.min, stats2.min), Math.max(stats1.max, stats2.max));
				}
			});
		}

		private static class StatsResults {

			private final float min;
			private final float max;

			public StatsResults(float min, float max) {
				this.min = min;
				this.max = max;
			}
		}

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.out.println("Wrong number of arguments, usage is: <input_path>");
			return;
		}

		Job job = new Job();
		job.run(args[0]);
	}
}
