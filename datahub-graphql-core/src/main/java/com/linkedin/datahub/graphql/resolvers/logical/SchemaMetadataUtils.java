package com.linkedin.datahub.graphql.resolvers.logical;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.NullType;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.Schemaless;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import com.linkedin.schema.UnionType;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Builds a {@link SchemaMetadata} aspect for a logical model from user-provided columns. */
public class SchemaMetadataUtils {

  private SchemaMetadataUtils() {}

  @Nonnull
  public static SchemaMetadata buildSchemaMetadata(
      @Nonnull final String schemaName,
      @Nonnull final DataPlatformUrn platform,
      @Nonnull final List<EditableSchemaFieldInput> columns) {
    final SchemaFieldArray fields = new SchemaFieldArray();
    for (EditableSchemaFieldInput column : columns) {
      final SchemaField field =
          new SchemaField()
              .setFieldPath(column.getFieldPath())
              .setType(mapSchemaFieldDataType(column.getType()))
              // nativeDataType is required on the aspect but meaningless for a logical model;
              // derive it from the canonical type so the user never has to supply it.
              .setNativeDataType(column.getType().toString());
      fields.add(field);
    }
    return new SchemaMetadata()
        .setSchemaName(schemaName)
        .setPlatform(platform)
        .setVersion(0L)
        .setHash("")
        .setPlatformSchema(SchemaMetadata.PlatformSchema.create(new Schemaless()))
        .setFields(fields);
  }

  @Nonnull
  public static SchemaFieldDataType mapSchemaFieldDataType(
      @Nonnull final com.linkedin.datahub.graphql.generated.SchemaFieldDataType type) {
    switch (type) {
      case BYTES:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()));
      case FIXED:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new FixedType()));
      case BOOLEAN:
        return new SchemaFieldDataType()
            .setType(SchemaFieldDataType.Type.create(new BooleanType()));
      case STRING:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
      case NUMBER:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
      case DATE:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType()));
      case TIME:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
      case ENUM:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType()));
      case NULL:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType()));
      case ARRAY:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
      case MAP:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType()));
      case STRUCT:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));
      case UNION:
        return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new UnionType()));
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized SchemaFieldDataType provided %s", type));
    }
  }

  /** Validates the column list is non-empty and has unique field paths. */
  public static void validateColumns(@Nonnull final List<EditableSchemaFieldInput> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("A logical model must have at least one column.");
    }
    final Set<String> seen = new HashSet<>();
    for (EditableSchemaFieldInput column : columns) {
      if (!seen.add(column.getFieldPath())) {
        throw new IllegalArgumentException(
            String.format("Duplicate column field path: %s", column.getFieldPath()));
      }
    }
  }

  /**
   * A change is breaking unless every existing column survives with the same field path AND the
   * same type. Removals, renames, and retypes are breaking; additions and reorders are not.
   */
  public static boolean isBreakingChange(
      @Nonnull final SchemaMetadata existing, @Nonnull final SchemaMetadata updated) {
    return !breakingParentPaths(existing, updated).isEmpty();
  }

  /**
   * Returns the set of existing parent field paths that a schema change breaks: any path that was
   * removed, renamed (the old path disappears), or retyped. Additions and reorders leave every
   * existing path with its original type and so contribute nothing. A change is breaking iff this
   * set is non-empty; the set names exactly which child column mappings must be torn down.
   */
  @Nonnull
  public static Set<String> breakingParentPaths(
      @Nonnull final SchemaMetadata existing, @Nonnull final SchemaMetadata updated) {
    final Map<String, SchemaFieldDataType> updatedByPath =
        updated.getFields().stream()
            .collect(
                Collectors.toMap(SchemaField::getFieldPath, SchemaField::getType, (a, b) -> a));
    final Set<String> affected = new HashSet<>();
    for (SchemaField existingField : existing.getFields()) {
      final SchemaFieldDataType updatedType = updatedByPath.get(existingField.getFieldPath());
      if (updatedType == null || !updatedType.equals(existingField.getType())) {
        affected.add(existingField.getFieldPath());
      }
    }
    return affected;
  }

  /** Reads a dataset's current schemaMetadata aspect, or null if absent. */
  @Nullable
  public static SchemaMetadata readSchemaMetadata(
      @Nonnull final EntityClient entityClient,
      @Nonnull final OperationContext opContext,
      @Nonnull final com.linkedin.common.urn.Urn urn)
      throws Exception {
    final EntityResponse response =
        entityClient.getV2(
            opContext,
            Constants.DATASET_ENTITY_NAME,
            urn,
            Collections.singleton(Constants.SCHEMA_METADATA_ASPECT_NAME));
    if (response == null
        || !response.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
      return null;
    }
    return new SchemaMetadata(
        response.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());
  }

  /** Returns the field paths of a dataset's current schema (empty if it has none). */
  @Nonnull
  public static List<String> fieldPathsOf(
      @Nonnull final EntityClient entityClient,
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn)
      throws Exception {
    return fieldPathsOf(entityClient, opContext, Collections.singleton(urn))
        .getOrDefault(urn, new ArrayList<>());
  }

  /**
   * Returns the field paths of each dataset's current schema in a single batched fetch (empty list
   * for a dataset with no schema). Replaces per-child reads with one {@code batchGetV2} call.
   */
  @Nonnull
  public static Map<Urn, List<String>> fieldPathsOf(
      @Nonnull final EntityClient entityClient,
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<Urn> urns)
      throws Exception {
    final Map<Urn, List<String>> result = new HashMap<>();
    if (urns.isEmpty()) {
      return result;
    }
    final Map<Urn, EntityResponse> responses =
        entityClient.batchGetV2(
            opContext,
            Constants.DATASET_ENTITY_NAME,
            new HashSet<>(urns),
            Collections.singleton(Constants.SCHEMA_METADATA_ASPECT_NAME));
    for (Urn urn : urns) {
      final List<String> paths = new ArrayList<>();
      final EntityResponse response = responses.get(urn);
      if (response != null
          && response.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
        final SchemaMetadata schema =
            new SchemaMetadata(
                response.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());
        schema.getFields().forEach(f -> paths.add(f.getFieldPath()));
      }
      result.put(urn, paths);
    }
    return result;
  }

  /**
   * Reads, for a page of physical children, the live column mappings that point at {@code
   * parentUrn}. Returns per child a map of {@code childFieldPath -> parentFieldPath}, restricted to
   * schema-field logicalParent edges whose destination is a field of {@code parentUrn} (mappings to
   * other logical parents are ignored). Runs in two batched fetches for the whole page: one for the
   * children's schemas, one for their schema-field logicalParent aspects.
   */
  @Nonnull
  public static Map<Urn, Map<String, String>> childColumnMappings(
      @Nonnull final EntityClient entityClient,
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn parentUrn,
      @Nonnull final Collection<Urn> childUrns)
      throws Exception {
    final Map<Urn, Map<String, String>> mappings = new HashMap<>();
    if (childUrns.isEmpty()) {
      return mappings;
    }

    final Map<Urn, List<String>> childFieldPaths = fieldPathsOf(entityClient, opContext, childUrns);

    // Map each child's schema-field URN back to its (childUrn, childFieldPath) so the fetched
    // logicalParent aspects can be attributed to the right child column.
    final Map<Urn, Pair<Urn, String>> fieldUrnToChildField = new HashMap<>();
    final Set<Urn> childFieldUrns = new HashSet<>();
    for (Urn childUrn : childUrns) {
      mappings.put(childUrn, new LinkedHashMap<>());
      for (String childFieldPath : childFieldPaths.getOrDefault(childUrn, new ArrayList<>())) {
        final Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(childUrn, childFieldPath);
        fieldUrnToChildField.put(childFieldUrn, Pair.of(childUrn, childFieldPath));
        childFieldUrns.add(childFieldUrn);
      }
    }
    if (childFieldUrns.isEmpty()) {
      return mappings;
    }

    final Map<Urn, EntityResponse> fieldResponses =
        entityClient.batchGetV2(
            opContext,
            Constants.SCHEMA_FIELD_ENTITY_NAME,
            childFieldUrns,
            Collections.singleton(Constants.LOGICAL_PARENT_ASPECT_NAME));

    for (Map.Entry<Urn, Pair<Urn, String>> entry : fieldUrnToChildField.entrySet()) {
      final EntityResponse response = fieldResponses.get(entry.getKey());
      if (response == null
          || !response.getAspects().containsKey(Constants.LOGICAL_PARENT_ASPECT_NAME)) {
        continue;
      }
      final LogicalParent logicalParent =
          new LogicalParent(
              response.getAspects().get(Constants.LOGICAL_PARENT_ASPECT_NAME).getValue().data());
      if (!logicalParent.hasParent() || logicalParent.getParent() == null) {
        continue;
      }
      final Urn parentFieldUrn = logicalParent.getParent().getDestinationUrn();
      final Optional<Pair<Urn, String>> parsed =
          SchemaFieldUtils.parseSchemaFieldUrn(parentFieldUrn);
      // Keep only edges that resolve to a field of the model whose schema is changing; a child may
      // simultaneously map columns to other logical parents and those must be left untouched.
      if (parsed.isEmpty() || !parentUrn.equals(parsed.get().getFirst())) {
        continue;
      }
      final Urn childUrn = entry.getValue().getFirst();
      final String childFieldPath = entry.getValue().getSecond();
      mappings.get(childUrn).put(childFieldPath, parsed.get().getSecond());
    }
    return mappings;
  }
}
