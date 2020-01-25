package com.hari.gradle.spark.plugin.tasks;

import static com.hari.gradle.spark.plugin.Constants.DISTRIBUTED_YARN_CACHE_PATH;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_USER_NAME;
import static com.hari.gradle.spark.plugin.Constants.SPARK_CONF_DEPLOY_MODE;
import static com.hari.gradle.spark.plugin.Constants.SPARK_CONF_YARN_ZIP;
import static com.hari.gradle.spark.plugin.Constants.SPARK_MAIN_CLASSPATH;
import static com.hari.gradle.spark.plugin.Constants.YARN_CONF_DIR;
import static com.hari.gradle.spark.plugin.SPGLogger.PROPERTY_SET_VALUE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.spark.launcher.SparkLauncher;

import com.hari.gradle.spark.plugin.SPGLogger;

/**
 * The main class which would execute the SparkJob via SparkLauncher.
 * 
 * @author harim
 *
 */

public class LaunchMainSpark {

	/**
	 * @param args[0]
	 *            - Name of the spark app which is executed.
	 * @param args[1]
	 *            - Master (can be local , cluster)
	 * @param args[2]
	 *            - Jar containing the spark application to be run , added as
	 *            appResource.
	 * @param args[3]
	 *            - The class which contains the spark job representing the
	 *            application.
	 * @param args[4]
	 *            - classPath for driver and executor.
	 * @param args[5]
	 *            - File to which standard error will be re-directed to.
	 * @param args[6]
	 *            - File to which standard out will be re-directed to.
	 * @param args[7]
	 *            - Spark Home location.
	 * @param args[8]
	 *            - Spark user overridden configs.
	 */

	private static final Predicate<String> isValidConf = conf -> conf != null && !conf.isEmpty()
			&& !conf.equals("EMPTY");

	public static final Function<String, List<String>> parseConf = conf -> asList(conf.split("\\s*,\\s*spark."))
			.stream().filter(con -> con != null).map(con1 -> con1.trim()).filter(con2 -> !con2.isEmpty())
			.map(parsed -> "spark." + parsed).collect(toList());

	public static final Function<String, List<String>> parseConf2 = conf -> asList(conf.split(",")).stream()
			.filter(Objects::isNull).filter(conf2 -> conf2.contains("=")).collect(toList());

	public static void main(String args[]) {
		if (args == null || args.length != 8)
			throw new IllegalArgumentException(
					String.format("Number of program args violated , expected 8 args but actual is %s", args.length));
		String appName = args[0];
		String master = args[1];
		String jarPath = args[2];
		String mainClass = args[3];
		String errFile = args[4];
		String outFile = args[5];
		String sparkHome = args[6];
		String sparkConfig = args[7];

		// look for if env vars YARN_CONF_DIR and HADOOP_HOME have been set
		// if so it would be needed to submit application to yarn.
		Map<String, String> envs = Environment.toMap(
				Environment.newInstance(YARN_CONF_DIR, System.getenv(YARN_CONF_DIR)),
				Environment.newInstance(HADOOP_HOME, System.getenv(HADOOP_HOME)),
				Environment.newInstance(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME)));
		String yarnDistributedClassPath = System.getenv(DISTRIBUTED_YARN_CACHE_PATH);
		String classPath = System.getenv(SPARK_MAIN_CLASSPATH);
		String yarnClusterMode = System.getenv(SPARK_CONF_DEPLOY_MODE);
		Optional<SparkLauncher> launcher = Optional
				.ofNullable(!envs.isEmpty() && envs.size() == 3 ? new SparkLauncher(envs) : new SparkLauncher());
		try {
			Process launch = launcher.map(launch1 -> launch1.setAppName(appName).setMaster(master)
					.setMainClass(mainClass).setSparkHome(sparkHome).setAppResource(jarPath)
					.redirectError(new File(errFile)).redirectOutput(new File(outFile)).setVerbose(true))
					.map(launch2 -> {
						if (yarnDistributedClassPath != null && !yarnDistributedClassPath.isEmpty()) {
							SPGLogger.logFine.accept(
									"Launching the application in yarn mode and that requires uploading the deps to distributed yarn cache");
							SPGLogger.logFine
									.accept(PROPERTY_SET_VALUE.apply(SPARK_CONF_YARN_ZIP, yarnDistributedClassPath));
							return launch2.setConf(SPARK_CONF_YARN_ZIP, yarnDistributedClassPath)
									.setDeployMode(yarnClusterMode);
						} else {
							// In stand-alone mode be sure to set the Driver and Executor classPath.
							return launch2.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classPath)
									.setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classPath);
						}
					}).map(launch3 -> {
						// parse out the comma separated spark configs
						// each parsed record contains key separated by "=" followed by its value.
						buildSparkConf(sparkConfig).forEach((key, value) -> launch3.setConf(key, value));
						return launch3;
					}).get().launch();
			launch.waitFor();
			int exit = launch.exitValue();
			if (exit == 0)
				SPGLogger.logInfo.accept("Spark job completed successfully ");
			else if (exit == 1) {
				SPGLogger.logError.accept("Spark job terminated with errors , please check stdErr.txt and stdOut.txt ");
				throw new RuntimeException("Spark application failed");
			}
		} catch (IOException | InterruptedException e) {
			SPGLogger.logError.accept("LaunchMainSpark exited with an error " + e.getLocalizedMessage());
		}
	}

	/***
	 * Function to parse spark config into corresponding key value confs.
	 * 
	 * @param sparkConfig
	 *            - unparsed spark configuration as String.
	 * @return Map of key value pairs.
	 */

	public static Map<String, String> buildSparkConf(String conf) {
		final List<String> parsedConf = Optional.ofNullable(conf).filter(isValidConf)
				.flatMap(conf1 -> Optional.of(parseConf2.apply(conf1))).orElse(Collections.emptyList());
		return Environment.toMap(parsedConf.stream().map(cnf -> {
			int eqIndex = cnf.indexOf("=");
			String key = cnf.substring(0, eqIndex);
			String value = cnf.substring(eqIndex + 1);
			if (key == null) {
				SPGLogger.logError.accept("Spark conf key cannot be null");
				throw new IllegalArgumentException("Incorrect spark config values, check the log for more details.");
			}
			return Environment.newInstance(key, value);
		}).collect(toList()));
	}

	public static class Environment {
		final String name;
		final String value;

		private Environment(String name, String value) {
			this.name = name;
			this.value = value;
		}

		public static Environment newInstance(String name, String value) {
			return new Environment(name, value);
		}

		public static Map<String, String> toMap(Environment... env) {
			return toMap(asList(env));
		}

		public static Map<String, String> toMap(List<Environment> env) {
			return env.stream().filter(en -> en.name != null && en.value != null)
					.collect(Collectors.toMap(e -> e.name, e -> e.value));
		}

	}

}
