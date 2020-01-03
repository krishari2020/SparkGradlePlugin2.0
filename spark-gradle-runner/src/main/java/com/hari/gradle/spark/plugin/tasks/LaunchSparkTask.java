package com.hari.gradle.spark.plugin.tasks;

import static com.hari.gradle.spark.plugin.Constants.DISTRIBUTED_YARN_CACHE_PATH;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_FS;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_USER_NAME;
import static com.hari.gradle.spark.plugin.Constants.JOB_DEPS_FILE_SUFFIX;
import static com.hari.gradle.spark.plugin.Constants.SPARK_CONF_DEPLOY_MODE;
import static com.hari.gradle.spark.plugin.Constants.SPARK_MAIN_CLASSPATH;
import static com.hari.gradle.spark.plugin.Constants.SPARK_SCALA_VERSION;
import static com.hari.gradle.spark.plugin.Constants.STD_ERR;
import static com.hari.gradle.spark.plugin.Constants.STD_OUT;
import static com.hari.gradle.spark.plugin.Constants.YARN_CONF_DIR;
import static com.hari.gradle.spark.plugin.SPGLogger.PROPERTY_SET_VALUE;
import static com.hari.gradle.spark.plugin.Settings.SETTINGS_EXTN;
import static com.hari.gradle.spark.plugin.SparkRunMode.getRunMode;
import static com.hari.gradle.spark.plugin.Utils.getFileSystem;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.JavaExecSpec;

import com.hari.gradle.spark.plugin.SPGLogger;
import com.hari.gradle.spark.plugin.Settings;
import com.hari.gradle.spark.plugin.SparkRunMode;

/**
 * Task to launch spark application via SparkLauncher. Depends on the
 * "CopyDepsTask" task to download all dependencies needed for driver and
 * executor classpath. SparkLauncher is then invoked by launching a separate
 * java process and the main class that would be invoked is
 * {@link com.hari.learning.gradle.spark.plugin.tasks.LaunchMainSpark } }.
 * 
 * Supported modes are 1) local 2) yarn - cluster 3) yarn - client
 * 
 * @author harim
 *
 */

public class LaunchSparkTask extends DefaultTask {

	public static final FilenameFilter JAR_FILTER = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(".jar");
		}
	};

	@TaskAction
	public void launch() throws IOException {
		final Project p = getProject();
		Settings settings = (Settings) p.getExtensions().getByName(SETTINGS_EXTN);
		String mainClass = settings.getMainClass();
		// mainClass cannot be empty.
		if (mainClass == null || mainClass.isEmpty())
			throw new IllegalArgumentException(" Main class cannot be empty");
		File libFolder = new File(p.getBuildDir().toPath().toString() + File.separator + "libs");
		File[] files = libFolder.listFiles(JAR_FILTER);
		if (files == null || files.length == 0)
			throw new IllegalArgumentException("Build failed with generating the output jar.");
		if (files.length != 1)
			throw new IllegalArgumentException("More than one version of the generated output jar");
		String sparkHome = settings.getSparkHome();
		if (sparkHome == null || sparkHome.isEmpty())
			throw new IllegalArgumentException("Spark home cannot be empty ");
		final String classPath = p.getBuildDir().toPath() + File.separator + JOB_DEPS_FILE_SUFFIX + File.separator
				+ "*";
		SPGLogger.logFine.accept(String.format("Classpath required for invoking SparkLauncher %s ", classPath));
		// determine the job needs to run in cluster or local machine.
		final SparkRunMode runMode = getRunMode.apply(settings.getMaster()).apply(settings.getMode());
		File errFile = (settings.getErrRedirect() != null && !settings.getErrRedirect().isEmpty())
				? new File(settings.getErrRedirect())
				: new File(p.getBuildDir().toPath() + File.separator + STD_ERR);
		SPGLogger.logInfo.accept(String.format("Error redirected to log file %s", errFile.toPath().toString()));
		File outFile = (settings.getErrRedirect() != null && !settings.getErrRedirect().isEmpty())
				? new File(settings.getErrRedirect())
				: new File(p.getBuildDir().toPath() + File.separator + STD_OUT);
		SPGLogger.logInfo.accept(String.format("Output redirected to log file %s", outFile.toPath().toString()));
		List<Object> prgArgs = asList(new Object[] { settings.getAppName(), settings.getMaster(),
				files[0].toPath().toString(), mainClass, errFile, outFile, sparkHome, settings.getSparkConfig() });
		SPGLogger.logFine.accept("Printing input args to LaunchMainSpark main method");
		p.javaexec(new Action<JavaExecSpec>() {
			@Override
			public void execute(JavaExecSpec spec) {
				spec.args(prgArgs);
				spec.setClasspath(
						p.fileTree(p.getLayout().getBuildDirectory().dir(JOB_DEPS_FILE_SUFFIX + File.separator)));
				SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply("SPARK_SCALA", settings.getScalaVersion()));
				spec.getEnvironment().put(SPARK_SCALA_VERSION, settings.getScalaVersion());
				SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(HADOOP_HOME, settings.getHadoopHome()));
				spec.getEnvironment().put(HADOOP_HOME, settings.getHadoopHome());
				SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(SPARK_MAIN_CLASSPATH, classPath));
				spec.getEnvironment().put(SPARK_MAIN_CLASSPATH, classPath);
				if (runMode == SparkRunMode.YARN_CLIENT || runMode == SparkRunMode.YARN_CLUSTER) {
					SPGLogger.logFine.accept(
							String.format("The spark job is to be submitted in yarn cluster and the deployMode is %s",
									settings.getMode()));
					spec.getEnvironment().put(HADOOP_USER_NAME, settings.getHadoopUserName());
					SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(HADOOP_USER_NAME, settings.getHadoopUserName()));
					spec.getEnvironment().put(YARN_CONF_DIR, settings.getHadoopConf());
					SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(YARN_CONF_DIR, settings.getHadoopConf()));
					String distYarnCachePath = new StringBuilder(
							getFileSystem(settings.getHadoopHome()).getConf().get(HADOOP_FS))
									.append(settings.getJarZipDestPath()).toString();
					spec.getEnvironment().put(DISTRIBUTED_YARN_CACHE_PATH, distYarnCachePath);
					SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(DISTRIBUTED_YARN_CACHE_PATH, distYarnCachePath));
					spec.getEnvironment().put(SPARK_CONF_DEPLOY_MODE, settings.getMode());
					SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(SPARK_CONF_DEPLOY_MODE, settings.getMode()));
				}
				spec.setMain(LaunchMainSpark.class.getCanonicalName());
			}
		});
	}
}
