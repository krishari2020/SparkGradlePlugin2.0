package com.hari.gradle.spark.plugin.tasks;

import com.hari.gradle.spark.plugin.SPGLogger;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.hari.gradle.spark.plugin.Constants.*;
import static com.hari.gradle.spark.plugin.SPGLogger.PROPERTY_SET_VALUE;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * The main class which would execute the SparkJob via SparkLauncher.
 *
 * @author harim
 */

public class LaunchMainSpark {

    public static final Function<String, List<String>> parseConf = conf -> asList(conf.split("\\s*,\\s*spark.")).stream().filter(con -> con != null).map(con1 -> con1.trim()).filter(con2 -> !con2.isEmpty()).map(parsed -> "spark." + parsed).collect(toList());
    public static final Function<String, List<String>> parseConf2 = conf -> asList(conf.split(";")).stream().filter(Objects::isNull).filter(conf2 -> conf2.contains("=")).collect(toList());
    /**
     * @param args[0]
     * - Name of the spark app to be executed.
     * @param args[1]
     * - Master (can be local , cluster)
     * @param args[2]
     * - Jar containing the spark application to be run , added as
     * appResource.
     * @param args[3]
     * - The class which contains the spark job representing the
     * application.
     * @param args[4]
     * - classPath for driver and executor.
     * @param args[5]
     * - File to which standard error will be re-directed to.
     * @param args[6]
     * - File to which standard out will be re-directed to.
     * @param args[7]
     * - Spark Home location.
     * @param args[8]
     * - Spark user overridden configs properties file path.
     */

    private static final Predicate<String> isValidConf = conf -> conf != null && !conf.isEmpty() && !conf.equals("EMPTY");

    public static void main(String args[]) {
        if (args == null || args.length != 9)
            throw new IllegalArgumentException(String.format("Number of program args violated , expected 9 args but actual is %s", args.length));
        String appName = args[0];
        String master = args[1];
        String jarPath = args[2];
        String mainClass = args[3];
        String errFile = args[4];
        String outFile = args[5];
        String sparkHome = args[6];
        String sparkConfFile = args[7];
        String dockerImageId = args[8];

        // look for if env vars YARN_CONF_DIR and HADOOP_HOME have been set
        // if so it would be needed to submit application to yarn.
        Map<String, String> envs = Environment.toMap(Environment.newInstance(YARN_CONF_DIR, System.getenv(YARN_CONF_DIR)), Environment.newInstance(HADOOP_HOME, System.getenv(HADOOP_HOME)), Environment.newInstance(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME)));
        String yarnDistributedClassPath = System.getenv(DISTRIBUTED_YARN_CACHE_PATH);
        String classPath = System.getenv(SPARK_MAIN_CLASSPATH);
        String yarnClusterMode = System.getenv(SPARK_CONF_DEPLOY_MODE);
        Optional<SparkLauncher> launcher = Optional.ofNullable(!envs.isEmpty() && envs.size() == 3 ? new SparkLauncher(envs) : new SparkLauncher());
        try {
            Process launch = launcher.map(launch1 -> launch1.setAppName(appName).setMaster(master).setMainClass(mainClass).setSparkHome(sparkHome).addJar(jarPath).setAppResource(jarPath).redirectError(new File(errFile)).redirectOutput(new File(outFile)).setVerbose(true)).map(launch2 -> {
                if (yarnDistributedClassPath != null && !yarnDistributedClassPath.isEmpty()) {
                    SPGLogger.logFine.accept("Launching the application in yarn mode and that requires uploading the deps to distributed yarn cache");
                    SPGLogger.logFine.accept(PROPERTY_SET_VALUE.apply(SPARK_CONF_YARN_ZIP, yarnDistributedClassPath));
                    return launch2.setConf(SPARK_CONF_YARN_ZIP, yarnDistributedClassPath).setDeployMode(yarnClusterMode);
                } else if (master.startsWith("k8s")) {
                    // Run on k8s cluster
                    if (dockerImageId == null || dockerImageId.isBlank())
                        throw new IllegalArgumentException("Docker image id cannot be empty or null when running" +
                                "in k8s");
                    launch2.setConf(SPARK_K8S_CONTAINER_IMAGE, dockerImageId);
                    return launch2;
                } else {
                    // In stand-alone mode be sure to set the Driver and Executor classPath.
                    launch2.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classPath).setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classPath);
                    return launch2;
                }
            }).map(launch3 -> {
                // if spark conf properties is provided then set
                // any error handling that is required needs to be handled by Spark Api
                if (sparkConfFile != null && !sparkConfFile.isBlank())
                    launch3.setPropertiesFile(sparkConfFile);
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
     * @param conf
     *            - unparsed spark configuration as String.
     * @return Map of key value pairs.
     */

    public static Map<String, String> buildSparkConf(String conf) {
        final List<String> parsedConf = Optional.ofNullable(conf).filter(isValidConf).flatMap(conf1 -> Optional.of(parseConf2.apply(conf1))).orElse(Collections.emptyList());
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
            return env.stream().filter(en -> en.name != null && en.value != null).collect(Collectors.toMap(e -> e.name, e -> e.value));
        }

    }

}
