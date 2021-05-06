package com.linkedin.experimental;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.pegasus.generator.DataSchemaParser;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelProcessorTask extends DefaultTask {

    @Option(option = "modelPath", description = "The root directory to read PDL models from.")
    String modelPath;

    @Option(option = "resolverPath", description = "The root directory to read PDL models from.")
    FileCollection resolverPath;

    @TaskAction
    public void generateArtifacts() throws IOException {
        // First, create a SchemaParser
        final String resolverPathAsStr = resolverPath.getAsPath();
        System.out.println(resolverPathAsStr);

        final DataSchemaParser parser = new DataSchemaParser(resolverPathAsStr);
        final DataSchemaParser.ParseResult result = parser.parseSources(new String[]{ modelPath });

        // Parsed schemas success --> Now time to look through.
        final DataSchema snapshotSchema = parser.getSchemaResolver().existingDataSchema("com.linkedin.metadata.snapshot.Snapshot");

        // Alternatively, we can go through each and find the @entity annotations. For now let's keep this.
        // Now that we have reference, we can go through each snapshot, object generate the appropriate mappings
        // Need to do a DFS

        final UnionDataSchema snapshotUnionSchema = (UnionDataSchema) snapshotSchema.getDereferencedDataSchema();
        final List<UnionDataSchema.Member> unionMembers = snapshotUnionSchema.getMembers();

        for (UnionDataSchema.Member member : unionMembers) {
            generateArtifactForEntitySnapshot(member.getType());
        }
    }

    private void generateArtifactForEntitySnapshot(final DataSchema entitySnapshotSchema) {
        // #0 Validate the Snapshot definition
        final RecordDataSchema entitySnapshotRecordSchema = validateSnapshot(entitySnapshotSchema);

        // #1 Parse information about the entity from the "entity" annotation.
        final Map<String, Object> entityAnnotation = getProperty(entitySnapshotRecordSchema, "Entity", Map.class, false);
        // Only generate information when the @Entity annotation is present.
        if (entityAnnotation != null) {
            final String entityName = (String) entityAnnotation.get("name");
            final Boolean searchable = (Boolean) entityAnnotation.get("searchable");
            final Boolean browsable = (Boolean) entityAnnotation.get("browsable");

            // Now that we have these, we can create an entity spec.
            final EntitySpec entitySpec = new EntitySpec(entityName, searchable, browsable, null);

            final ArrayDataSchema aspectArraySchema = (ArrayDataSchema) entitySnapshotRecordSchema.getField("aspects").getType().getDereferencedDataSchema();
            final UnionDataSchema aspectUnionSchema = (UnionDataSchema) aspectArraySchema.getItems().getDereferencedDataSchema();

            final List<UnionDataSchema.Member> unionMembers = aspectUnionSchema.getMembers();
            for (UnionDataSchema.Member member : unionMembers) {
                generateArtifactsFromEntityAspect(entitySpec, member.getType());
            }
        }
    }

    private void generateArtifactsFromEntityAspect(final EntitySpec entitySpec, final DataSchema aspect) {
        final RecordDataSchema aspectRecordSchema = validateAspect(aspect);
        // #1 Understand whether we should traverse in using the @Aspect annotation.
        final Map<String, Object> aspectAnnotation = getProperty(aspectRecordSchema, "Aspect", Map.class, false);
        if (aspectAnnotation != null) {
            final String aspectName = (String) aspectAnnotation.get("name");

            // 1. Get the "Indexable" Annotations in a readable, validated format.
               // a. Can only appear on Array<Primitive> or Primitive fields.
               // b. Must provide specific values: Strategy, etc.
            final PathSpecToAnnotationTraverser<Map> visitor = new PathSpecToAnnotationTraverser<>(
                "Searchable",
                (obj) -> {
                    if (!Map.class.isAssignableFrom(obj.getClass())) {
                        throw new IllegalArgumentException("Failed to validate Searchable Annotation");
                    }

                    return Map.class.cast(obj);
                }
            );
            final DataSchemaRichContextTraverser traverser = new DataSchemaRichContextTraverser(visitor);
            traverser.traverse(aspectRecordSchema);
            final Map<List<String>, Map> result = visitor.getIndex();


            // 2. Get the "Relationship" Annotations in a readable, validated format.
              // a. Can only appear on RecordDataSchemas
              // b. RecordDataSchema must have a foreign key field. (Preferably an urn)

        }
    }

    private RecordDataSchema validateSnapshot(final DataSchema entitySnapshotSchema) {
        // 0. Validate that schema is a Record
        if (entitySnapshotSchema.getType() != DataSchema.Type.RECORD) {
            throw new IllegalArgumentException(String.format("Failed to validate entity snapshot schema of type %s. Schema must be of record type.", entitySnapshotSchema.getType().toString()));
        }
        final RecordDataSchema entitySnapshotRecordSchema = (RecordDataSchema) entitySnapshotSchema;

        // 1. Validate Urn field
        if (entitySnapshotRecordSchema.getField("urn") == null || entitySnapshotRecordSchema.getField("urn").getType().getDereferencedType() != DataSchema.Type.STRING) {
            throw new IllegalArgumentException(String.format("Failed to validate entity snapshot schema with name %s. Invalid urn field.", entitySnapshotRecordSchema.getName()));
        }

        // 2. Validate Aspect field
        if (entitySnapshotRecordSchema.getField("aspects") == null || entitySnapshotRecordSchema.getField("aspects").getType().getDereferencedType() != DataSchema.Type.ARRAY) {
            throw new IllegalArgumentException(String.format("Failed to validate entity snapshot schema with name %s. Invalid aspects field.", entitySnapshotRecordSchema.getName()));
        }
        return entitySnapshotRecordSchema;
    }

    private RecordDataSchema validateAspect(final DataSchema aspectSchema) {
        // 0. Validate that schema is a Record
        if (aspectSchema.getType() != DataSchema.Type.RECORD) {
            throw new IllegalArgumentException(String.format("Failed to validate aspect schema of type %s. Schema must be of record type.", aspectSchema.getType().toString()));
        }
        return (RecordDataSchema) aspectSchema;
    }

    private <T> T getProperty(final DataSchema schema, final String propertyName, final Class<T> classType, final boolean isRequired) {
        final Map<String, Object> props = schema.getProperties();
        if (props.containsKey(propertyName)) {
            final Object propObj = props.get(propertyName);
            if (classType.isAssignableFrom(propObj.getClass())) {
                return classType.cast(props.get(propertyName));
            }
            throw new IllegalArgumentException(String.format("Failed to cast annotation with name %s, value %s to specific type %s",
                    propertyName,
                    propObj.toString(),
                    classType.getName()));
        }
        if (isRequired) {
            throw new IllegalArgumentException(String.format("Failed to find annotation named %s in schema of type %s", propertyName, schema.getType().toString()));
        }
        return null;
    }
}
