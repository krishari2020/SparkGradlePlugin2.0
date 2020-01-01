package com.hari.gradle.spark.plugin.tasks;

import static com.hari.gradle.spark.plugin.Constants.DISTRIBUTED_YARN_CACHE_PATH;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_USER_NAME;
import static com.hari.gradle.spark.plugin.Constants.SPARK_CONF_DEPLOY_MODE;
import static com.hari.gradle.spark.plugin.Constants.SPARK_CONF_YARN_ZIP;
import static com.hari.gradle.spark.plugin.Constants.YARN_CONF_DIR;
import static com.hari.gradle.spark.plugin.SPGLogger.PROPERTY_SET_VALUE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

	public static void main(String args[]) {
		SPGLogger.logInfo.accept(String.format("Number of program args is %s",args.length));
		if (args == null || args.length != 9)
			throw new IllegalArgumentException(" Not enough arguments to launch SparkLauncher ");
		String appName = args[0];
		String master = args[1];
		String jarPath = args[2];
		String mainClass = args[3];
		String errFile = args[5];
		String outFile = args[6];
		String sparkHome = args[7];
		String sparkConfig = args[8];

		// look for if env vars YARN_CONF_DIR and HADOOP_HOME have been set
		// if so it would be needed to submit application to yarn.
		Map<String, String> envs = Environment.toMap(
				Environment.newInstance(YARN_CONF_DIR, System.getenv(YARN_CONF_DIR)),
				Environment.newInstance(HADOOP_HOME, System.getenv(HADOOP_HOME)),
				Environment.newInstance(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME)));
		String yarnDistributedClassPath = System.getenv(DISTRIBUTED_YARN_CACHE_PATH);
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
							return launch2.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, args[4])
									.setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, args[4]);
						}
					}).map(launch3 -> {
						// parse out the comma separated spark configs
						// each parsed record contains key separated by "=" followed by its value.
						if (sparkConfig == null || sparkConfig.isEmpty() || sparkConfig.equals("EMPTY"))
							return launch3;
						Map<String, String> sparkConfVars = Environment
								.toMap(asList(sparkConfig.split(",")).stream().map(conf -> conf.trim()).map(conf -> {
									String[] keyValue = conf.split("=");
									if (keyValue == null || keyValue.length != 2) {
										SPGLogger.logError.accept(String.format(" Incorrect key value property %s ",
												keyValue.toString()));
										throw new IllegalArgumentException(
												"Incorrect spark config values, check the log for more details.");
									}
									return Environment.newInstance(keyValue[0], keyValue[1]);
								}).collect(toList()));
						sparkConfVars.forEach((key, value) -> launch3.setConf(key, value));
						return launch3;
					}).get().launch();
			launch.waitFor();
			int exit = launch.exitValue();
			if (exit == 0)
				SPGLogger.logInfo.accept("Spark job completed successfully ");
			else if (exit == 1)
				SPGLogger.logInfo.accept("Spark job terminated with errors , please check stdErr.txt and stdOut.txt ");
		} catch (IOException | InterruptedException e) {
			SPGLogger.logError.accept("LaunchMainSpark exited with an error " + e.getLocalizedMessage());
		}
	}

	private static class Environment {
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
