package com.hari.gradle.spark.plugin.tests;

import java.util.Map;

import org.junit.Test;

import com.hari.gradle.spark.plugin.tasks.LaunchMainSpark;
import com.hari.gradle.spark.plugin.tasks.LaunchMainSpark.Environment;

/**
 * Test cases for spark config parsing.
 * 
 * @author harim
 *
 */

public class SparkConfParseTests {

	@Test
	public void testSimpleSparkConfValue() {
		final String sparkConf = "spark.driver.memory = 1g , spark.driver.memoryOverhead = 0.20 , spark.local.dir = '/tmp'";
		final Map<String, String> parsedSparkConf = LaunchMainSpark.buildSparkConf(sparkConf);
		// put some assert condition.
		// let me think all possible combinations.
		Map<String, String> result = Environment.toMap(Environment.newInstance("spark.driver.memory", "1g"),
				Environment.newInstance("spark.driver.memoryOverhead", "0.20"),
				Environment.newInstance("spark.local.dir", "/tmp"));
		result.entrySet().stream().allMatch(entry -> parsedSparkConf.containsKey(entry.getKey())
				&& parsedSparkConf.get(entry.getKey()).equals(entry.getValue()));
	}

	@Test
	public void testParseJavaOptions() {
		final String conf = "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps,spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps";
		final Map<String, String> parsedSparkConf = LaunchMainSpark.buildSparkConf(conf);
		// Expected values
		Map<String, String> result = Environment.toMap(
				Environment.newInstance("spark.executor.extraJavaOptions",
						"-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
				Environment.newInstance("spark.driver.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"));
		result.entrySet().stream().allMatch(entry -> parsedSparkConf.containsKey(entry.getKey())
				&& parsedSparkConf.get(entry.getKey()).equals(entry.getValue()));
	}

}
