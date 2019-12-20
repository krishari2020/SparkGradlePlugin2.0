package com.hari.gradle.spark.plugin;

import java.util.UUID;

/**
 * Settings required for launching spark job. Contains some default values as
 * well.
 * 
 * @author harim
 *
 */

public class Settings {

	public static final String SETTINGS_EXTN = "settings";

	private String hadoopHome = ""; // location of Hadoop Installation/ winutils.exe
	private String hadoopConf = ""; // location of Hadoop site.xml files.
	private String hadoopUserName = "root"; // by default run as root user.
	private String sparkHome;
	private String mainClass;
	private String appName = UUID.randomUUID().toString(); // provide a meaningful name if needed.
	private String master = "local[*]"; // default value is local mode.
	private String mode = ""; // default values is empty string since in local mode (default mode) it is NA.
	private String outRedirect;
	private String errRedirect;
	private String scalaVersion = "2.11"; // Default it to spark-scala 2.11 version.
	private String sparkConfig = "EMPTY"; // Add or override any spark configuration if required or else we
	// fall back to defaults.
	private String jarZipDestPath = "/tmp/spark_gradle_plugin/deps"; // default path where Yarn will
	// maintain a distributed cache of spark jar dependencies.

	public String getSparkConfig() {
		return sparkConfig;
	}

	public String getScalaVersion() {
		return scalaVersion;
	}

	public String getOutRedirect() {
		return outRedirect;
	}

	public String getErrRedirect() {
		return errRedirect;
	}

	public String getMaster() {
		return master;
	}

	public String getAppName() {
		return appName;
	}

	public String getHadoopHome() {
		return hadoopHome;
	}

	public String getMainClass() {
		return mainClass;
	}

	public String getSparkHome() {
		return sparkHome;
	}

	public String getMode() {
		return mode;
	}

	public String getJarZipDestPath() {
		return jarZipDestPath;
	}

	public void setHadoopHome(String hadoopHome) {
		this.hadoopHome = hadoopHome;
	}

	public void setSparkHome(String sparkHome) {
		this.sparkHome = sparkHome;
	}

	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setMaster(String master) {
		this.master = master;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public void setOutRedirect(String outRedirect) {
		this.outRedirect = outRedirect;
	}

	public void setErrRedirect(String errRedirect) {
		this.errRedirect = errRedirect;
	}

	public void setScalaVersion(String scalaVersion) {
		this.scalaVersion = scalaVersion;
	}

	public void setSparkConfig(String sparkConfig) {
		this.sparkConfig = sparkConfig;
	}

	public void setJarZipDestPath(String jarZipDestPath) {
		this.jarZipDestPath = jarZipDestPath;
	}

	public String getHadoopConf() {
		return hadoopConf;
	}

	public void setHadoopConf(String hadoopConf) {
		this.hadoopConf = hadoopConf;
	}

	public String getHadoopUserName() {
		return hadoopUserName;
	}

	public void setHadoopUserName(String hadoopUserName) {
		this.hadoopUserName = hadoopUserName;
	}

}
