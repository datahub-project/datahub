package com.linkedin.metadata.model.processor;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.pegasus.generator.DataSchemaParser;

import java.io.IOException;
import java.util.List;

public class ModelProcessorTask {

    private ModelProcessorTask() { }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: cmd <modelPath> <resolverPath>");
            System.exit(-1);
        }

        final String resolverPath = args[0];
        final String modelPath = args[1];

        final DataSchemaParser parser = new DataSchemaParser(resolverPath);
        parser.parseSources(new String[]{ modelPath });

        // Parsed schemas success --> Now time to look through.
        final DataSchema snapshotSchema = parser.getSchemaResolver().existingDataSchema("com.linkedin.metadata.snapshot.Snapshot");
        final List<EntitySpec> entitySpecs = EntitySpecBuilder.buildEntitySpecs(snapshotSchema);
    }
}