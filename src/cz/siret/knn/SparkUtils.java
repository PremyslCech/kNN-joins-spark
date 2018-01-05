package cz.siret.knn;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cz.siret.knn.model.Feature;

@SuppressWarnings("serial")
public class SparkUtils implements Serializable {

	/**
	 * Delete output directory if exists
	 * 
	 * @param configuration
	 * @param outputPath
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public void deleteOutputDirectory(Configuration configuration, String outputPath) throws IOException, URISyntaxException {

		FileSystem hdfs = FileSystem.get(new URI(outputPath), configuration);
		Path outputDir = new Path(outputPath);
		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}
	}

	/**
	 * Reads input path and converts each line to a feature
	 * 
	 * @param javaSparkContext
	 * @param input
	 * @param minPartitions
	 * @return
	 */
	public JavaRDD<Feature> readInputPath(JavaSparkContext javaSparkContext, String input, int minPartitions) {

		return javaSparkContext.textFile(input, minPartitions).map(new Function<String, Feature>() {
			public Feature call(String line) {
				int index = line.indexOf(',');
				if (index > 0) {
					line = line.substring(index + 1);
				}

				return Feature.parse(line);
			}
		});
	}

}
