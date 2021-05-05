package com.linkedin.experimental;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

public class ModelProcessorTask extends DefaultTask {

    @Option(option="baseModelPath", description="The root directory to read PDL models from.")
    String baseModelPath;

    @TaskAction
    public void generateArtifacts() {
        System.out.println(baseModelPath);
    }
}
