package com.hari.gradle.spark.plugin.tasks;

import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_HOME_DIR;
import static com.hari.gradle.spark.plugin.Constants.HADOOP_USER_NAME;
import static com.hari.gradle.spark.plugin.Constants.JOB_DEPS_FILE_SUFFIX;
import static com.hari.gradle.spark.plugin.Constants.YARN_CONF_DIR;
import static com.hari.gradle.spark.plugin.Constants.YARN_LIB_ZIP_FILE;
import static com.hari.gradle.spark.plugin.SPGLogger.PROPERTY_SET_VALUE;
import static com.hari.gradle.spark.plugin.Settings.SETTINGS_EXTN;
import static com.hari.gradle.spark.plugin.SparkRunMode.getRunMode;
import static com.hari.gradle.spark.plugin.Utils.getFileSystem;
import static com.hari.gradle.spark.plugin.tasks.LaunchSparkTask.JAR_FILTER;
import static java.util.Arrays.asList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskAction;

import com.hari.gradle.spark.plugin.SPGLogger;
import com.hari.gradle.spark.plugin.Settings;
import com.hari.gradle.spark.plugin.SparkRunMode;

/**
 * Prepares spark-job submission to cluster (currently only YARN). Steps involve
 * copying jar dependencies as archive file into HDFS location and also set ENV
 * variables which would be required by subsequent {@link LaunchSparkTask} task
 * which does launch Spark job via SparkLauncher.
 * 
 * @author harim
 *
 */

public class PrepareForClusterSubmit extends DefaultTask {

	@TaskAction
	public void zipAndUploadToCluster() throws Exception {
		final Project p = getProject();
		Settings settings = (Settings) p.getExtensions().getByName(SETTINGS_EXTN);

		final SparkRunMode runMode = getRunMode.apply(settings.getMaster()).apply(settings.getMode());
		if (runMode == SparkRunMode.YARN_CLIENT || runMode == SparkRunMode.YARN_CLUSTER) {
			// Require HADOOP_CONF_DIR to be set to run in cluster.
			if (settings.getHadoopHome() == null || settings.getHadoopHome().length() == 0)
				throw new IllegalArgumentException("Invalid hadoopHome property value");
			// compress all the needed jars into a zip file before uploading it to cluster.
			// the required jars are not more than what is required for driver/executor
			// classpath. The zip file would be created under ${PROJECT_BUILD_DIR} as
			// ${YARN_LIB_ZIP_FILE}
			final String hadoopConf = settings.getHadoopConf();
			final String depsDownloadedPath = p.getBuildDir().toPath() + File.separator + JOB_DEPS_FILE_SUFFIX
					+ File.separator;
			SPGLogger.logInfo.accept(
					String.format("Required runtime jars downloaded in this path %s which would be bundled as zip",
							depsDownloadedPath));
			final String zipPath = p.getBuildDir().toPath().toString() + File.separator + YARN_LIB_ZIP_FILE;
			final String destJarPath = settings.getJarZipDestPath();

			// also set the hadoop.home.dir system property and hadoop_user_name property
			// and yarn_conf_dir.

			System.setProperty(HADOOP_HOME_DIR, settings.getHadoopHome());
			System.setProperty(HADOOP_HOME, settings.getHadoopHome());
			System.setProperty(HADOOP_USER_NAME, settings.getHadoopUserName());
			System.setProperty(YARN_CONF_DIR, settings.getHadoopConf());
			// Although I would like reduce significantly the size of the zip file
			// I am not so familiar with algos for compressing files which are already
			// compressed in the form of jar. Tried with few compression strategies
			// but it turned out to be futile :(
			try (final ZipOutputStream zipOS = new ZipOutputStream(
					new CheckedOutputStream(new FileOutputStream(zipPath), new CRC32()))) { // need to understand more
																							// about Adler32 | CRC32.
				zipOS.setLevel(Deflater.BEST_COMPRESSION);
				List<File> sparkJars = asList(new File(depsDownloadedPath).listFiles(JAR_FILTER));
				sparkJars.forEach(jar -> {
					try {
						zipOS.putNextEntry(new ZipEntry(jar.getName()));
						byte[] bytes = Files.readAllBytes(Paths.get(jar.toURI()));
						zipOS.write(bytes, 0, bytes.length);
						zipOS.closeEntry();
					} catch (IOException io) {
						SPGLogger.logError.accept("Failed while creating the yarn_libs zip file. ");
						throw new RuntimeException(io);
					}
				});
			}
			// Post successful creation of archive file it needs to be uploaded to HDFS
			if (!Files.exists(Paths.get(zipPath)))
				throw new FileNotFoundException("yarn_libs not found , hence failing the task");

			int result = ToolRunner.run(new HDFSCopier(), new String[] { hadoopConf, zipPath, destJarPath });
			if (result != 0) {
				SPGLogger.logError.accept("Failed to copy yarn_libs zip to HDFS , hence failing the task");
				throw new RuntimeException("HDFS copy operation failed with exit code" + result);
			}

		}

	}

	/**
	 * {@link org.apache.hadoop.util.Tool} implementation for copying spark deps as
	 * zipped file from local FS to Hadoop FS. Arguments passed would be args[0] -
	 * Hadoop Home args[1] - Input path in local FS. args[2] - Output path in remote
	 * FS.
	 * 
	 */

	class HDFSCopier extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
			String hadoopHome = args[0];
			String inputPath = args[1];
			String outPath = args[2];
			SPGLogger.logFine.accept("Printing the arguments passed to HDFSCopier ");
			SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply("Path containing the hadoop configuration", hadoopHome));
			SPGLogger.logFine.accept(PROPERTY_SET_VALUE
					.apply("Input zip file containing jars required for driver/executor classpath", inputPath));
			SPGLogger.logFine
					.accept(PROPERTY_SET_VALUE.apply("HDFS location to which zip file is uploaded to", outPath));
			final FileSystem hadoopFs = getFileSystem(hadoopHome);
			hadoopFs.copyFromLocalFile(new Path(inputPath), new Path(outPath));
			hadoopFs.setPermission(new Path(outPath), new FsPermission("777"));
			SPGLogger.logInfo.accept(String.format("Copied jars archive from %s to HDFS location %s",
					inputPath.toString(), outPath.toString()));
			hadoopFs.close();
			return 0;
		}

	}
}
