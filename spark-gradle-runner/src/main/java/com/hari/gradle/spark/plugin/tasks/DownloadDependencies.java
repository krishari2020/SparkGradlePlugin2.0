package com.hari.gradle.spark.plugin.tasks;

import com.hari.gradle.spark.plugin.SPGLogger;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.CopySpec;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.function.Function;

import static com.hari.gradle.spark.plugin.Constants.JOB_DEPS_FILE_SUFFIX;

/**
 * Downloads spark and other required dependencies into
 * {@literal "build/sparkDeps"}
 *
 * @author harim
 */

public class DownloadDependencies extends DefaultTask {

    public static Function<Project, String> getJobDepPath = project -> new StringBuilder(project.getBuildDir().toPath().toString()).append(File.separator)
            .append(JOB_DEPS_FILE_SUFFIX).toString();

    @TaskAction
    public void downloadDeps() {
        Project p = getProject();
        // download all compile-time/run-time dependencies into folder
        // ${BUILD_DIR/jobDeps}
        final Configuration compileDeps = p.getConfigurations().getByName("compile");
        final Configuration runtimeDeps = p.getConfigurations().getByName("runtime");
        final String jobDepsPath = getJobDepPath.apply(p);
        p.copy(new Action<CopySpec>() {
            @Override
            public void execute(CopySpec copySpec) {
                SPGLogger.logInfo.accept(
                        String.format("Local path for downloading all required jars for spark job is %s", jobDepsPath));
                copySpec.into(jobDepsPath);
                copySpec.from(compileDeps);
                copySpec.from(runtimeDeps);
            }
        });
    }

}
