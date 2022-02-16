package com.hari.gradle.spark.plugin;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Represents spark's run-mode configuration Contains "master" as well as
 * "deployMode" information.
 *
 * @author harim
 */

public enum SparkRunMode {

    LOCAL("local[*]", ""), YARN_CLUSTER("yarn", "cluster"),
    YARN_CLIENT("yarn", "client"), K8S_CLUSTER("k8s", "cluster"),
    K8S_CLIENT("k8s", "client");

    /**
     * Utils method to fetch the appropriate SparkRunMode value based on master and
     * deployMode value.
     */

    public static Function<String, Function<String, SparkRunMode>> getRunMode = master -> deployMode -> {
        List<SparkRunMode> runModes = asList(values()).stream()
                .filter(srm -> srm.master.equals(master) && srm.deployMode.equals(deployMode)).collect(toList());
        if (runModes.size() != 1) {
            throw new IllegalStateException("Ambigous, should match only one SparkRunMode enum instance");
        }
        return runModes.get(0);
    };
    private final String master;
    private final String deployMode;

    SparkRunMode(String master, String deployMode) {
        this.master = master;
        this.deployMode = deployMode;
    }

    public String getMaster() {
        return master;
    }

    public String getDeployMode() {
        return deployMode;
    }
}
