package com.hari.gradle.spark.plugin.tasks.kubernetes;

import com.hari.gradle.spark.plugin.Settings;
import com.hari.gradle.spark.plugin.tasks.DownloadDependencies;
import org.apache.commons.io.FileUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * Gradle task to copy dependencies and the build jar into Docker base directory.
 * Is enabled only in k8s mode.
 *
 * @author harikrishna
 */

public class CopyDepsToDockerBaseDir extends DefaultTask {

    @TaskAction
    public boolean copyDepsToDockerBaseDirectory() {
        Project p = getProject();
        Settings s = Settings.getSettings(p);
        File dockerBaseDir = s.getDockerBaseDirectory();
        File targetDepsDir = new File(dockerBaseDir, "jobDeps");
        File jobDepsDir = new File(DownloadDependencies.getJobDepPath.apply(p));
        try {
            FileUtils.copyFile(jobDepsDir, targetDepsDir);
        } catch (IOException ioe) {
            throw new RuntimeException(String.format("Failed while" +
                    " copying jobDeps: %s to Docker base dir %s", jobDepsDir, targetDepsDir));
        }
        // Copy the scala compiled jar to DockerBaseDirectory
        File buildFile = p.getBuildFile();
        File buildFileTarget = new File(dockerBaseDir, buildFile.getName());
        if (buildFile == null || !buildFile.getName().endsWith(".jar"))
            throw new RuntimeException(String.format("The build file is invalid %s",
                    Optional.of(buildFile).map(File::getName).orElse("")));
        try {
            FileUtils.copyFile(buildFile, buildFileTarget);
        } catch (IOException io) {
            throw new RuntimeException(String.format("Failed while copying build jar " +
                            "from %s to %s due to the following exception", buildFile.getAbsolutePath().toString()
                    , buildFileTarget.getAbsolutePath()));
        }
        return true;
    }

}
