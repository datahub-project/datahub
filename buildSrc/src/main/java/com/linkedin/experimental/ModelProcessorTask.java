package com.linkedin.experimental;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.pegasus.generator.DataSchemaParser;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.List;

public class ModelProcessorTask extends DefaultTask {

    @Option(option = "modelPath", description = "The root directory to read PDL models from.")
    String modelPath;

    @Option(option = "resolverPath", description = "The root directory to read PDL models from.")
    FileCollection resolverPath;

    @Option(option = "outputDir", description = "The output directory to write generated metadata to.")
    String outputDir;

    @TaskAction
    public void generateArtifacts() throws IOException {
        // First, create a SchemaParser
        final String resolverPathAsStr = resolverPath.getAsPath();

        final DataSchemaParser parser = new DataSchemaParser(resolverPathAsStr);
        parser.parseSources(new String[]{ modelPath });

        // Parsed schemas success --> Now time to look through.
        final DataSchema snapshotSchema = parser.getSchemaResolver().existingDataSchema("com.linkedin.metadata.snapshot.Snapshot");
        final List<EntitySpec> entitySpecs = EntitySpecBuilder.buildEntitySpecs(snapshotSchema);
    }
}
