package com.hari.gradle.spark.plugin.tasks;

import static com.hari.gradle.spark.plugin.Constants.JOB_DEPS_FILE_SUFFIX;

import java.io.File;

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.CopySpec;
import org.gradle.api.tasks.TaskAction;

import com.hari.gradle.spark.plugin.SPGLogger;

/**
 * Downloads spark and other required dependencies into
 * {@literal "build/sparkDeps"}
 * 
 * @author harim
 *
 */

public class DownloadDependencies extends DefaultTask {

	@TaskAction
	public void downloadDeps() {
		Project p = getProject();
		// download all compile-time/run-time dependencies into folder
		// ${BUILD_DIR/jobDeps}
		final Configuration compileDeps = p.getConfigurations().getByName("compile");
		final Configuration runtimeDeps = p.getConfigurations().getByName("runtime");
		p.copy(new Action<CopySpec>() {
			@Override
			public void execute(CopySpec copySpec) {
				String jobDepsPath = new StringBuilder(p.getBuildDir().toPath().toString()).append(File.separator)
						.append(JOB_DEPS_FILE_SUFFIX).toString();
				SPGLogger.logInfo.accept(
						String.format("Local path for downloading all required jars for spark job is %s", jobDepsPath));
				copySpec.into(jobDepsPath);
				copySpec.from(compileDeps);
				copySpec.from(runtimeDeps);
			}
		});
	}

}
