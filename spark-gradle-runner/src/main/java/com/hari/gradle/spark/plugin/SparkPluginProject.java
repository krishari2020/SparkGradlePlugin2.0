package com.hari.gradle.spark.plugin;

import static com.hari.gradle.spark.plugin.Settings.SETTINGS_EXTN;
import static com.hari.gradle.spark.plugin.SparkRunMode.getRunMode;
import static java.util.Arrays.asList;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.repositories.MavenArtifactRepository;
import org.gradle.api.specs.Spec;

import com.hari.gradle.spark.plugin.tasks.CopyDepsTask;
import com.hari.gradle.spark.plugin.tasks.LaunchSparkTask;
import com.hari.gradle.spark.plugin.tasks.PrepareForClusterSubmit;

/**
 * Gradle plugin to build and launch a spark application , this was created out
 * of the necessity of skipping so many mundane repetitive steps to deploy a
 * spark application especially in clusters. Intends to support three modes. 1)
 * local - Drivers and executors run on the same node. 2) client-mode - Driver
 * runs in the node launching the application and executors run in the cluster
 * nodes. 3) cluster-mode - Driver runs in one of the nodes in the cluster and
 * executors run in the cluster as well.
 * 
 * @author harim
 *
 */

public class SparkPluginProject implements Plugin<Project> {

	private static final String GROUP = "Spark Runner";

	@Override
	public void apply(Project p) {
		p.getPluginManager().apply(org.gradle.api.plugins.scala.ScalaPlugin.class);
		p.getRepositories().addAll(asList(p.getRepositories().mavenCentral(), p.getRepositories().mavenLocal(),
				p.getRepositories().maven(new Action<MavenArtifactRepository>() {
					@Override
					public void execute(MavenArtifactRepository customRepo) {
						try {
							customRepo.setUrl(new URI("http://repo.gradle.org/gradle/libs-releases-local"));
						} catch (URISyntaxException urise) {
							SPGLogger.logError.accept("Incorrect URL provided");
						}
					}
				})));
		final Settings settings = p.getExtensions().create(SETTINGS_EXTN, Settings.class);
		Task copyDeps = p.getTasks().create("copyDeps", CopyDepsTask.class);
		copyDeps = taskInit.apply(copyDeps).apply("Copies all dependencies required to run spark job")
				.apply(p.task("jar")).apply(GROUP);

		Task prepClusterSubmit = p.getTasks().create("prepareClusterSubmit", PrepareForClusterSubmit.class);
		prepClusterSubmit = taskInit.apply(prepClusterSubmit)
				.apply(" Copies all spark deps into the cluster to create a distributed cache in Yarn").apply(copyDeps)
				.apply(GROUP);
		prepClusterSubmit.onlyIf(new Spec<Task>() {
			@Override
			public boolean isSatisfiedBy(Task arg0) {
				String master = settings.getMaster();
				String mode = settings.getMode();
				SparkRunMode spkRunMode = getRunMode.apply(master).apply(mode);
				return spkRunMode == SparkRunMode.YARN_CLIENT || spkRunMode == SparkRunMode.YARN_CLUSTER;
			}
		});
		Task launch = p.getTasks().create("launch", LaunchSparkTask.class);
		launch = taskInit.apply(launch).apply("Launch a spark-job with overrided settings").apply(prepClusterSubmit)
				.apply(GROUP);
	}

	private static interface GradleTaskInitializer
			extends Function<Task, Function<String, Function<Task, Function<String, Task>>>> {
	}

	private GradleTaskInitializer taskInit = task -> description -> depTask -> taskGroup -> {
		task.setDescription(description);
		task.dependsOn(depTask);
		task.setGroup(taskGroup);
		return task;
	};

}
