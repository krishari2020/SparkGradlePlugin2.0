package com.hari.gradle.spark.plugin;

import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME_DIR;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class serving api's across tasks and plugin packages.
 * 
 * @author harim
 *
 */

public class Utils {

	// observable side effect.
	// Lambda for setting a system property as given a "key" and a "value".
	public static BiConsumer<String, String> setProperty = (property, value) -> System.setProperty(property, value);
	// Lambda which returns a FileSystem object if Hadoop_home and HadoopConf value
	// is applied.
	public static Function<String, Function<String, FileSystem>> getFS = hadoopHome -> hadoopConf -> {
		setProperty.accept(HADOOP_HOME, hadoopHome);
		setProperty.accept(HADOOP_HOME_DIR, hadoopHome);
		return getFileSystem(hadoopConf);
	};

	private static FileSystem getFileSystem(String hadoopConf) {
		File siteFiles = new File(hadoopConf);
		try {
			return FileSystem.get(asList(siteFiles.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String file) {
					return file.endsWith(".xml");
				}
			})).stream().map(file -> new Path(file.toPath().toAbsolutePath().toString())).reduce(new Configuration(),
					(conf, site) -> {
						conf.addResource(site);
						return conf;
					}, (c1, c2) -> c2));
		} catch (IOException ioe) {
			SPGLogger.logError.accept("Failed while retrieving Hadoop FileSystem object");
			throw new RuntimeException(ioe);
		}
	}

}
