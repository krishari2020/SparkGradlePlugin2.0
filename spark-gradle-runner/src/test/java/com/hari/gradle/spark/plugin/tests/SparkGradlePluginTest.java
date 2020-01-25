package com.hari.gradle.spark.plugin.tests;

import static com.hari.gradle.spark.plugin.tests.ReadWriteUtils.readFromFile;
import static com.hari.gradle.spark.plugin.tests.ReadWriteUtils.writeToFile;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hari.gradle.spark.plugin.SPGLogger;

/**
 * Plugin integration test and run 'launcSpark' task in local mode with sample
 * build.gradle file under src/test/resources/build.gradle.
 * 
 * @author harim
 *
 */
// TODO write test cases for submitting job onto the cluster.

public class SparkGradlePluginTest {

	@Rule
	public final TemporaryFolder tempProjDir = new TemporaryFolder();
	private File settingsFile;
	private File buildLocalFile;
	private File sourceJsonFile;

	@Before
	public void setup() throws IOException {
		String settingsFileContent = readFromFile.apply(new File("src/test/resources/local/input/settings.gradle"));
		settingsFile = tempProjDir.newFile("settings.gradle");
		sourceJsonFile = tempProjDir.newFile("source.json");
		String sourceJsonContent = readFromFile.apply(new File("src/test/resources/local/input/source.json"));
		writeToFile.accept(sourceJsonFile, sourceJsonContent);
		writeToFile.accept(settingsFile, settingsFileContent);
		buildLocalFile = tempProjDir.newFile("build.gradle");
	}

	@Test
	public void testInLocalMode() throws IOException {
		String buildLocalContent = readFromFile.apply(new File("src/test/resources/local/input/build.gradle"));
		writeToFile.accept(buildLocalFile, buildLocalContent);
		BuildResult result = GradleRunner.create().withProjectDir(tempProjDir.getRoot()).withPluginClasspath()
				.withArguments("launchSpark", "-i").forwardOutput()
				.forwardStdError(new FileWriter(new File("src/test/resources/local/output/stdErr.txt")))
				.forwardStdOutput(new FileWriter(new File("src/test/resources/local/output/stdOut.txt"))).build();
		SPGLogger.logInfo.accept(String.format("Outcome of the task is %s", result.task(":launchSpark").getOutcome()));
		assertEquals(TaskOutcome.SUCCESS, result.task(":launchSpark").getOutcome());
	}

	public static class FilterEmployeeBfromSource {

		private static final String TARGET_FILE = "employeeOfB.json";

		public static void main(String[] args) {
			SparkSession spark = SparkSession.builder().getOrCreate();
			Dataset<Row> sourceDf = spark.read().json("source1.json");
			sourceDf.filter(sourceDf.col("employer").equalTo("B")).write().mode(SaveMode.Overwrite).json(TARGET_FILE);
		}
	}

}
