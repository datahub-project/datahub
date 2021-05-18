package com.linkedin.metadata.models;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.RelationshipAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EntitySpecBuilder {

  private static final String URN_FIELD_NAME = "urn";
  private static final String ASPECTS_FIELD_NAME = "aspects";

  public static SchemaAnnotationHandler _searchHandler = new AspectSchemaAnnotationHandler(SearchableAnnotation.ANNOTATION_NAME);
  public static SchemaAnnotationHandler _browseHandler = new AspectSchemaAnnotationHandler("Browsable");
  public static SchemaAnnotationHandler _relationshipHandler = new AspectSchemaAnnotationHandler(RelationshipAnnotation.ANNOTATION_NAME);

  private EntitySpecBuilder() { }

  public enum ValidationMode {
    /**
     * Warn if a model cannot be validated.
     */
    WARN,
    /**
     * Throw an exception if a model cannot be validated.
     */
    THROW,
  }

  public static List<EntitySpec> buildEntitySpecs(final DataSchema snapshotSchema, final ValidationMode validationMode) {

    final UnionDataSchema snapshotUnionSchema = (UnionDataSchema) snapshotSchema.getDereferencedDataSchema();
    final List<UnionDataSchema.Member> unionMembers = snapshotUnionSchema.getMembers();

    final List<EntitySpec> entitySpecs = new ArrayList<>();
    for (final UnionDataSchema.Member member : unionMembers) {
      final EntitySpec entitySpec = buildEntitySpec(member.getType(), validationMode);
      if (entitySpec != null) {
        entitySpecs.add(buildEntitySpec(member.getType(), validationMode));
      }
    }
    return entitySpecs;
  }

  public static EntitySpec buildEntitySpec(final DataSchema entitySnapshotSchema, final ValidationMode validationMode) {
    // 0. Validate the Snapshot definition
    final RecordDataSchema entitySnapshotRecordSchema = validateSnapshot(entitySnapshotSchema, validationMode);

    if (entitySnapshotRecordSchema == null) {
      return null;
    }

    // 1. Parse information about the entity from the "entity" annotation.
    final Object entityAnnotationObj = entitySnapshotRecordSchema.getProperties().get(EntityAnnotation.ANNOTATION_NAME);

    if (entityAnnotationObj != null) {

      final ArrayDataSchema aspectArraySchema =
          (ArrayDataSchema) entitySnapshotRecordSchema.getField(ASPECTS_FIELD_NAME).getType().getDereferencedDataSchema();

      final UnionDataSchema aspectUnionSchema =
          (UnionDataSchema) aspectArraySchema.getItems().getDereferencedDataSchema();

      final List<UnionDataSchema.Member> unionMembers = aspectUnionSchema.getMembers();
      final List<AspectSpec> aspectSpecs = new ArrayList<>();
      for (final UnionDataSchema.Member member : unionMembers) {
        final AspectSpec spec = buildAspectSpec(member.getType(), validationMode);
        if (spec != null) {
          aspectSpecs.add(spec);
        }
      }

      return new EntitySpec(
          aspectSpecs,
          EntityAnnotation.fromSchemaProperty(entityAnnotationObj, entitySnapshotRecordSchema.getFullName()),
          entitySnapshotRecordSchema,
          (TyperefDataSchema) aspectArraySchema.getItems());
    }

    failValidation(String.format("Could not build entity spec for entity with name %s. Missing @%s annotation.",
        entitySnapshotRecordSchema.getName(), EntityAnnotation.ANNOTATION_NAME), validationMode);
    return null;
  }

  private static AspectSpec buildAspectSpec(final DataSchema aspectDataSchema, final ValidationMode validationMode) {

    final RecordDataSchema aspectRecordSchema = validateAspect(aspectDataSchema, validationMode);

    if (aspectRecordSchema == null) {
      return null;
    }

    final Object aspectAnnotationObj = aspectRecordSchema.getProperties().get(AspectAnnotation.ANNOTATION_NAME);

    if (aspectAnnotationObj != null) {

      final AspectAnnotation aspectAnnotation =
          AspectAnnotation.fromSchemaProperty(aspectAnnotationObj, aspectRecordSchema.getFullName());

      final SchemaAnnotationProcessor.SchemaAnnotationProcessResult processedSearchResult =
          SchemaAnnotationProcessor.process(Collections.singletonList(_searchHandler),
              aspectRecordSchema, new SchemaAnnotationProcessor.AnnotationProcessOption());

      final SchemaAnnotationProcessor.SchemaAnnotationProcessResult processedRelationshipResult =
          SchemaAnnotationProcessor.process(Collections.singletonList(_relationshipHandler),
              aspectRecordSchema, new SchemaAnnotationProcessor.AnnotationProcessOption());

      final SchemaAnnotationProcessor.SchemaAnnotationProcessResult processedBrowseResult =
          SchemaAnnotationProcessor.process(Collections.singletonList(_browseHandler),
              aspectRecordSchema, new SchemaAnnotationProcessor.AnnotationProcessOption());

      // Extract Searchable Field Specs
      final SearchableFieldSpecExtractor searchableFieldSpecExtractor = new SearchableFieldSpecExtractor(processedSearchResult);
      final DataSchemaRichContextTraverser searchableFieldSpecTraverser =
          new DataSchemaRichContextTraverser(searchableFieldSpecExtractor);
      searchableFieldSpecTraverser.traverse(aspectRecordSchema);

      // Extract Relationship Field Specs
      final RelationshipFieldSpecExtractor relationshipFieldSpecExtractor = new RelationshipFieldSpecExtractor(processedRelationshipResult);
      final DataSchemaRichContextTraverser relationshipFieldSpecTraverser =
          new DataSchemaRichContextTraverser(relationshipFieldSpecExtractor);
      relationshipFieldSpecTraverser.traverse(aspectRecordSchema);

      // Extract Browsable Field Specs
      final BrowsePathFieldSpecExtractor browsePathFieldSpecExtractor = new BrowsePathFieldSpecExtractor(processedBrowseResult);
      final DataSchemaRichContextTraverser browsePathFieldSpecTraverser =
          new DataSchemaRichContextTraverser(browsePathFieldSpecExtractor);
      browsePathFieldSpecTraverser.traverse(aspectRecordSchema);

      return new AspectSpec(
          aspectAnnotation,
          searchableFieldSpecExtractor.getSpecs(),
          relationshipFieldSpecExtractor.getSpecs(),
          browsePathFieldSpecExtractor.getSpecs(),
          aspectRecordSchema);
    }

    failValidation(String.format("Could not build aspect spec for aspect with name %s. Missing @Aspect annotation.",
        aspectRecordSchema.getName()), validationMode);
    return null;
  }

  private static RecordDataSchema validateSnapshot(final DataSchema entitySnapshotSchema, final ValidationMode validationMode) {
    // 0. Validate that schema is a Record
    if (entitySnapshotSchema.getType() != DataSchema.Type.RECORD) {
      failValidation(String.format("Failed to validate entity snapshot schema of type %s. Schema must be of record type.",
          entitySnapshotSchema.getType().toString()), validationMode);
      return null;
    }

    final RecordDataSchema entitySnapshotRecordSchema = (RecordDataSchema) entitySnapshotSchema;

    // 1. Validate Urn field
    if (entitySnapshotRecordSchema.getField(URN_FIELD_NAME) == null
        || entitySnapshotRecordSchema.getField(URN_FIELD_NAME).getType().getDereferencedType() != DataSchema.Type.STRING) {
      failValidation(String.format("Failed to validate entity snapshot schema with name %s. Invalid urn field.",
          entitySnapshotRecordSchema.getName()), validationMode);
      return null;
    }

    // 2. Validate Aspect Array
    if (entitySnapshotRecordSchema.getField(ASPECTS_FIELD_NAME) == null
        || entitySnapshotRecordSchema.getField(ASPECTS_FIELD_NAME).getType().getDereferencedType() != DataSchema.Type.ARRAY) {

      failValidation(String.format("Failed to validate entity snapshot schema with name %s. Invalid aspects field found. 'aspects' should be an array of union type.",
          entitySnapshotRecordSchema.getName()), validationMode);
      return null;
    }

    // 3. Validate Aspect Union
    final ArrayDataSchema aspectArray = (ArrayDataSchema) entitySnapshotRecordSchema.getField(ASPECTS_FIELD_NAME).getType().getDereferencedDataSchema();
    if (aspectArray.getItems().getType() != DataSchema.Type.TYPEREF
       || aspectArray.getItems().getDereferencedType() != DataSchema.Type.UNION) {

      failValidation(String.format("Failed to validate entity snapshot schema with name %s. Invalid aspects field field. 'aspects' should be an array of union type.",
          entitySnapshotRecordSchema.getName()), validationMode);
      return null;
    }

    return entitySnapshotRecordSchema;
  }

  private static RecordDataSchema validateAspect(final DataSchema aspectSchema, final ValidationMode validationMode) {
    // 0. Validate that schema is a Record
    if (aspectSchema.getType() != DataSchema.Type.RECORD) {

      failValidation(String.format("Failed to validate aspect schema of type %s. Schema must be of record type.",
          aspectSchema.getType().toString()), validationMode);
      return null;

    }
    return (RecordDataSchema) aspectSchema;
  }

  private static void failValidation(final String message, final ValidationMode validationMode) {
    if (ValidationMode.THROW.equals(validationMode)) {
      throw new ModelValidationException(message);
    } else {
      log.warn(message);
    }
  }
}
