package com.hari.gradle.spark.plugin.tasks.kubernetes;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.BuildImageCmd;
import com.github.dockerjava.api.command.BuildImageResultCallback;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.hari.gradle.spark.plugin.Settings;
import org.apache.commons.io.FileUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Gradle task to build Docker image required by Spark's Driver pod
 * to launch Spark application. Is enabled only in k8s mode.
 *
 * @author harikrishna
 */

public class BuildDockerImage extends DefaultTask {

    private static final long TIMEOUT = 20000L;

    @TaskAction
    public void buildImage() {
        Project p = getProject();
        Settings settings = Settings.getSettings(p);
        Properties docker = settings.getDockerProperties();
        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().withProperties(docker).build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder().dockerHost(config.getDockerHost()).sslConfig(config.getSSLConfig()).maxConnections(100).connectionTimeout(Duration.ofSeconds(30)).responseTimeout(Duration.ofSeconds(45)).build();
        // Move the Dockerfile template from resources to a designated folder.
        File dockerFile = settings.getDockerFile();
        File dockerBaseDirectory = settings.getDockerBaseDirectory();
        File dockerFileTgt = new File(dockerBaseDirectory, "Dockerfile");
        try {
            FileUtils.copyFile(dockerFile, dockerFileTgt);
        } catch (IOException io) {
            throw new RuntimeException("Failed while copying docker template to docker file", io);
        }
        DockerClient client = DockerClientImpl.getInstance(config, httpClient);
        BuildImageCmd buildImageCmd = client.buildImageCmd(dockerFile);
        BuildImageResultCallback result = buildImageCmd.withBaseDirectory(dockerBaseDirectory).withTags(Collections.singleton(settings.getDockerImageTag())).start();
        settings.setDockerImageTag(result.awaitImageId());
    }

}
