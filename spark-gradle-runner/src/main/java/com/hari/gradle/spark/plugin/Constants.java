package com.hari.gradle.spark.plugin;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates all constants required.
 *
 * @author harim
 *
 */

public class Constants {

	// Hadoop related constants
	public static final String HADOOP_HOME = "HADOOP_HOME";
	public static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
	public static final String HADOOP_HOME_DIR = "hadoop.home.dir";
	public static final String HADOOP_FS = "fs.defaultFS";
	public static final String HDFS_URL_PREFIX = "hdfs://";
	public static final Map<String, String> HADOOP_FS_CONF;
	static {
		HADOOP_FS_CONF = new HashMap<>();
		HADOOP_FS_CONF.put("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		HADOOP_FS_CONF.put("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}

	// Yarn related constants
	public static final String YARN_CONF_DIR = "YARN_CONF_DIR";
	public static final String YARN_LIB_ZIP_FILE = "yarn_libs.zip";
	public static final String DISTRIBUTED_YARN_CACHE_PATH = "DISTRIBUTED_YARN_CACHE_PATH";

	// Spark related constants
	public static final String STD_ERR = "stdErr.txt";
	public static final String STD_OUT = "stdOut.txt";
	public static final String SPARK_SCALA_VERSION = "SPARK_SCALA_VERSION";
	public static final String SPARK_CONF_YARN_ZIP = "spark.yarn.archive";
	public static final String SPARK_CONF_DEPLOY_MODE = "deployMode";
	public static final String SPARK_HOME = "SPARK_HOME";

	// Spark-Gradle related constants
	public static final String JOB_DEPS_FILE_SUFFIX = "jobDeps";

	private Constants() {
	}

}
