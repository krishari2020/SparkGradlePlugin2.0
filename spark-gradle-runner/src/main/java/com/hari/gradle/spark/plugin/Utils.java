package com.hari.gradle.spark.plugin;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class serving api's across tasks and plugin packages.
 * 
 * 
 * @author harim
 *
 */

public class Utils {

	/**
	 * Returns a {@link org.apache.hadoop.fs.FileSystem} instance
	 *
	 * @param hadoopHome
	 *            - Path containing necessary *-site.xml files needed to communicate
	 *            with the cluster.
	 * @return
	 */
	public static FileSystem getFileSystem(String hadoopHome) {

		File siteFiles = new File(hadoopHome);
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
