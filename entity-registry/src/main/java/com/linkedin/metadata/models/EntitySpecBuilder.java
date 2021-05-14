package com.linkedin.metadata.models;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.ArrayList;
import java.util.List;


public class EntitySpecBuilder {

  private EntitySpecBuilder() {
  }

  public static List<EntitySpec> buildEntitySpecs(final DataSchema snapshotSchema) {

    final UnionDataSchema snapshotUnionSchema = (UnionDataSchema) snapshotSchema.getDereferencedDataSchema();
    final List<UnionDataSchema.Member> unionMembers = snapshotUnionSchema.getMembers();

    final List<EntitySpec> entitySpecs = new ArrayList<>();
    for (final UnionDataSchema.Member member : unionMembers) {
      final EntitySpec entitySpec = buildEntitySpec(member.getType());
      if (entitySpec != null) {
        entitySpecs.add(buildEntitySpec(member.getType()));
      }
    }
    return entitySpecs;
  }

  public static EntitySpec buildEntitySpec(final DataSchema entitySnapshotSchema) {
    // 0. Validate the Snapshot definition
    final RecordDataSchema entitySnapshotRecordSchema = validateSnapshot(entitySnapshotSchema);

    // 1. Parse information about the entity from the "entity" annotation.
    final Object entityAnnotationObj = entitySnapshotRecordSchema.getProperties().get("Entity");

    if (entityAnnotationObj != null) {

      final ArrayDataSchema aspectArraySchema =
          (ArrayDataSchema) entitySnapshotRecordSchema.getField("aspects").getType().getDereferencedDataSchema();
      final UnionDataSchema aspectUnionSchema =
          (UnionDataSchema) aspectArraySchema.getItems().getDereferencedDataSchema();
      final List<UnionDataSchema.Member> unionMembers = aspectUnionSchema.getMembers();

      final List<AspectSpec> aspectSpecs = new ArrayList<>();
      for (UnionDataSchema.Member member : unionMembers) {
        final AspectSpec spec = getAspectSpec(member.getType());
        if (spec != null) {
          aspectSpecs.add(spec);
        }
      }

      return new EntitySpec(aspectSpecs, EntityAnnotation.fromSchemaProperty(entityAnnotationObj),
          entitySnapshotRecordSchema, (TyperefDataSchema) aspectArraySchema.getItems());
    }
    // TODO: Replace with exception once we are ready.
    System.out.println(
        String.format("Warning: Could not build entity spec for entity with name %s. Missing @Entity annotation.",
            entitySnapshotRecordSchema.getName()));
    return null;
  }

  private static AspectSpec getAspectSpec(final DataSchema aspect) {
    final RecordDataSchema aspectRecordSchema = validateAspect(aspect);
    // #1 Understand whether we should traverse in using the @Aspect annotation.
    final Object aspectAnnotationObj = aspectRecordSchema.getProperties().get("Aspect");

    if (aspectAnnotationObj != null) {

      final AspectAnnotation aspectAnnotation =
          AspectAnnotation.fromSchemaProperty(aspectAnnotationObj);

      // Extract Searchable Field Specs
      final SearchableFieldSpecExtractor searchableFieldSpecExtractor = new SearchableFieldSpecExtractor();
      final DataSchemaRichContextTraverser searchableFieldSpecTraverser =
          new DataSchemaRichContextTraverser(searchableFieldSpecExtractor);
      searchableFieldSpecTraverser.traverse(aspectRecordSchema);

      // Extract Relationship Field Specs
      final RelationshipFieldSpecExtractor relationshipFieldSpecExtractor = new RelationshipFieldSpecExtractor();
      final DataSchemaRichContextTraverser relationshipFieldSpecTraverser =
          new DataSchemaRichContextTraverser(relationshipFieldSpecExtractor);
      relationshipFieldSpecTraverser.traverse(aspectRecordSchema);

      // Extract Browsable Field Specs
      final BrowsePathFieldSpecExtractor browsePathFieldSpecExtractor = new BrowsePathFieldSpecExtractor();
      final DataSchemaRichContextTraverser browsePathFieldSpecTraverser =
          new DataSchemaRichContextTraverser(browsePathFieldSpecExtractor);
      browsePathFieldSpecTraverser.traverse(aspectRecordSchema);

      return new AspectSpec(aspectAnnotation, searchableFieldSpecExtractor.getSpecs(),
          relationshipFieldSpecExtractor.getSpecs(), browsePathFieldSpecExtractor.getSpecs(), aspectRecordSchema);
    }
    // TODO: Replace with exception once we are ready.
    System.out.println(
        String.format("Warning: Could not build aspect spec for aspect with name %s. Missing @Aspect annotation.",
            aspectRecordSchema.getName()));
    return null;
  }

  private static RecordDataSchema validateSnapshot(final DataSchema entitySnapshotSchema) {
    // 0. Validate that schema is a Record
    if (entitySnapshotSchema.getType() != DataSchema.Type.RECORD) {
      throw new IllegalArgumentException(
          String.format("Failed to validate entity snapshot schema of type %s. Schema must be of record type.",
              entitySnapshotSchema.getType().toString()));
    }
    final RecordDataSchema entitySnapshotRecordSchema = (RecordDataSchema) entitySnapshotSchema;

    // 1. Validate Urn field
    if (entitySnapshotRecordSchema.getField("urn") == null
        || entitySnapshotRecordSchema.getField("urn").getType().getDereferencedType() != DataSchema.Type.STRING) {
      throw new IllegalArgumentException(
          String.format("Failed to validate entity snapshot schema with name %s. Invalid urn field.",
              entitySnapshotRecordSchema.getName()));
    }

    // 2. Validate Aspect field
    if (entitySnapshotRecordSchema.getField("aspects") == null
        || entitySnapshotRecordSchema.getField("aspects").getType().getDereferencedType() != DataSchema.Type.ARRAY) {
      throw new IllegalArgumentException(
          String.format("Failed to validate entity snapshot schema with name %s. Invalid aspects field.",
              entitySnapshotRecordSchema.getName()));
    }
    return entitySnapshotRecordSchema;
  }

  private static RecordDataSchema validateAspect(final DataSchema aspectSchema) {
    // 0. Validate that schema is a Record
    if (aspectSchema.getType() != DataSchema.Type.RECORD) {
      throw new IllegalArgumentException(
          String.format("Failed to validate aspect schema of type %s. Schema must be of record type.",
              aspectSchema.getType().toString()));
    }
    return (RecordDataSchema) aspectSchema;
  }
}
