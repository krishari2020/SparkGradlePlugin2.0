package com.hari.gradle.spark.plugin;

import org.gradle.api.Project;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

/**
 * Settings required for launching spark job. Contains some default values as
 * well.
 *
 * @author harim
 */

public class Settings {

    private static final String SETTINGS_EXTN = "settings";
    private static Function<Project, Settings> createSettings = p ->
            p.getExtensions().create(SETTINGS_EXTN, Settings.class);
    private static Settings settings;
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
    private String sparkConfig = "EMPTY";
    // maintain a distributed cache of spark jar dependencies.
    // fall back to defaults.
    private String jarZipDestPath = "/tmp/spark_gradle_plugin/deps"; // default path where Yarn will
    //Spark Kubernetes specific properties, will be accessed only if the cluster is k8s
    private String dockerPropertiesPath; // location of the property file containing required props to create Docker client.
    private Properties dockerProperties; // the Properties object encapsulating Docker specific properties.
    private String dockerFilePath; // location of the Dockerfile that is used to build the Docker image used by Driver pod to launch Application.
    private String dockerBaseDirectoryPath; // base location of docker related artifacts to build the image used by Driver pod.
    private File dockerFile; // the DockerFile itself.
    private File dockerBaseDirectory; // Base directory of Docker context.
    private String dockerImageTag; // Tag to be associated to the Docker image that is built.
    private String sparkConfFilePath; // Path of the properties file containing Spark's conf

    public static Settings getSettings(Project p) {
        if (p == null)
            throw new IllegalArgumentException(" Project parameter cannot be passed a null value");
        if (settings == null) {
            createSettings.apply(p);
            settings = (Settings) p.getExtensions().getByName(SETTINGS_EXTN);
        }
        return settings;
    }

    public String getScalaVersion() {
        return scalaVersion;
    }

    public void setScalaVersion(String scalaVersion) {
        this.scalaVersion = scalaVersion;
    }

    public String getOutRedirect() {
        return outRedirect;
    }

    public void setOutRedirect(String outRedirect) {
        this.outRedirect = outRedirect;
    }

    public String getErrRedirect() {
        return errRedirect;
    }

    public void setErrRedirect(String errRedirect) {
        this.errRedirect = errRedirect;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getHadoopHome() {
        return hadoopHome;
    }

    public void setHadoopHome(String hadoopHome) {
        this.hadoopHome = hadoopHome;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public void setSparkHome(String sparkHome) {
        this.sparkHome = sparkHome;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getJarZipDestPath() {
        return jarZipDestPath;
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

    public String getSparkConfig() {
        return sparkConfig;
    }

    public void setSparkConfig(String sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    public Properties getDockerProperties() {
        if (dockerProperties == null) {
            if (dockerPropertiesPath == null || dockerPropertiesPath.isBlank())
                throw new IllegalArgumentException(String.format("Docker Properties path is not set to a valid path %s", dockerPropertiesPath));
            try (FileInputStream fis = new FileInputStream(new File(dockerPropertiesPath))) {
                dockerProperties = new Properties();
                dockerProperties.load(fis);
                return dockerProperties;
            } catch (FileNotFoundException fne) {
                throw new RuntimeException(String.format("No properties path found in the given path %s", dockerPropertiesPath), fne);
            } catch (IOException ioe) {
                throw new RuntimeException(String.format("Exception while reading docker properties file in the given path %s", dockerPropertiesPath), ioe);
            }
        }
        return dockerProperties;
    }

    public void setDockerProperties(Properties dockerProperties) {
        this.dockerProperties = dockerProperties;
    }

    public String getDockerPropertiesPath() {
        return dockerPropertiesPath;
    }

    public void setDockerPropertiesPath(String dockerPropertiesPath) {
        this.dockerPropertiesPath = dockerPropertiesPath;
    }

    public String getDockerFilePath() {
        if (dockerFilePath == null || dockerFilePath.isBlank())
            throw new IllegalStateException("DockerFile path is null or empty");
        return dockerFilePath;
    }

    public void setDockerFilePath(String dockerFilePath) {
        this.dockerFilePath = dockerFilePath;
    }

    public File getDockerFile() {
        if (dockerFile == null) {
            dockerFile = new File(dockerFilePath);
        }
        return dockerFile;
    }

    public File getDockerBaseDirectory() {
        if (dockerBaseDirectory == null) {
            dockerBaseDirectory = new File(dockerBaseDirectoryPath);
        }
        return dockerBaseDirectory;
    }

    public void setDockerBaseDirectory(File dockerBaseDirectory) {
        this.dockerBaseDirectory = dockerBaseDirectory;
    }

    public String getDockerBaseDirectoryPath(Project p) {
        if (dockerBaseDirectoryPath == null || dockerBaseDirectoryPath.isBlank()) {
            dockerBaseDirectoryPath = new File(p.getBuildDir(), "DockerArtifacts").getAbsolutePath();
        }
        return dockerBaseDirectoryPath;
    }

    public String getDockerImageTag() {
        return Optional.of(dockerImageTag).orElse("krishari2020/spark_gradle_plugin_image:v1");
    }

    public void setDockerImageTag(String dockerImageTag) {
        this.dockerImageTag = dockerImageTag;
    }

    public String getSparkConfFilePath() {
        return Optional.of(sparkConfFilePath).orElseGet(() -> {
            SPGLogger.logFine.accept("Does not contain spark conf overrides");
            return "";
        });
    }

    public void setSparkConfFilePath(String sparkConfFilePath) {
        this.sparkConfFilePath = sparkConfFilePath;
    }

}
