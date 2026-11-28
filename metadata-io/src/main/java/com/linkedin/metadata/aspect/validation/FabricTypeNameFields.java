package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * The environment fields of an entity registry: every top-level aspect field whose type is the
 * {@code FabricTypeName} typeref. Validation scope is derived from the schema, so switching a field
 * to {@code FabricTypeName} is the only step needed to have it validated.
 */
public final class FabricTypeNameFields {

  /** One environment field of one aspect. */
  @Value
  public static class Field {
    @Nonnull String entityName;
    @Nonnull String aspectName;
    @Nonnull String fieldName;
    boolean keyAspect;

    /** The search index field holding the value, or null when the field isn't searchable. */
    @Nullable String searchFieldName;
  }

  private static final String TYPED_URN_VALIDATOR =
      "com.linkedin.common.validator.TypedUrnValidator";

  private FabricTypeNameFields() {}

  /**
   * Finds every {@code FabricTypeName} field in the registry.
   *
   * @throws IllegalStateException when one is nested below the top level of an aspect, which the
   *     validators don't read
   */
  @Nonnull
  public static List<Field> discover(@Nonnull EntityRegistry entityRegistry) {
    List<Field> fields = new ArrayList<>();
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        RecordDataSchema schema = aspectSpec.getPegasusSchema();
        for (RecordDataSchema.Field field : schema.getFields()) {
          if (FabricTypeNames.isFabricTypeName(field.getType())) {
            fields.add(
                new Field(
                    entitySpec.getName(),
                    aspectSpec.getName(),
                    field.getName(),
                    aspectSpec.getName().equals(entitySpec.getKeyAspectName()),
                    searchFieldName(aspectSpec, field.getName())));
          } else {
            rejectNested(
                field.getType(),
                entitySpec.getName() + "." + aspectSpec.getName(),
                new HashSet<>());
          }
        }
      }
    }
    fields.sort(
        Comparator.comparing(Field::getEntityName)
            .thenComparing(Field::getAspectName)
            .thenComparing(Field::getFieldName));
    return fields;
  }

  /**
   * Aspects that reference entities whose URN holds an environment, through a typed URN field such
   * as {@code DatasetUrn} at any depth (e.g. upstream lineage). A typed URN holds an environment
   * when its {@code TypedUrnValidator} lists a {@code FabricTypeName} part.
   */
  @Value
  public static class UrnReferences {
    /** The (entity, aspect) pairs holding at least one such field. */
    @Nonnull List<AspectPluginConfig.EntityAspectName> aspects;

    /** Full name of each such typeref (e.g. com.linkedin.common.DatasetUrn) to its env part. */
    @Nonnull Map<String, Integer> envPartByTyperef;

    /** The typerefs whose env is the last part of the URN, which can be read without parsing. */
    @Nonnull Set<String> lastPartTyperefs;

    /**
     * Each record that holds such a URN at some depth, by full name, to the names of its fields
     * that lead to one. Lets {@link #customEnvs} skip every other field, such as the thousands of
     * columns of a schema.
     */
    @Nonnull Map<String, List<String>> referencingFields;

    public boolean contains(@Nonnull String entityName, @Nonnull String aspectName) {
      return aspects.stream()
          .anyMatch(
              pair ->
                  pair.getEntityName().equals(entityName)
                      && pair.getAspectName().equals(aspectName));
    }

    /**
     * The referenced URNs whose environment is not built-in, mapped to that environment. Only
     * fields leading to a typed URN are visited, and a URN of a built-in environment is recognized
     * without parsing. A URN that doesn't parse is left to the URN validators.
     */
    @Nonnull
    public Map<String, String> customEnvs(
        @Nonnull DataMap data, @Nonnull RecordDataSchema aspectSchema) {
      Map<String, String> found = new LinkedHashMap<>();
      scanRecord(data, aspectSchema, found);
      return found;
    }

    private void scan(
        @Nullable Object value, @Nullable DataSchema schema, @Nonnull Map<String, String> found) {
      if (value == null || schema == null) {
        return;
      }
      switch (schema.getType()) {
        case TYPEREF:
          TyperefDataSchema typeref = (TyperefDataSchema) schema;
          Integer part = envPartByTyperef.get(typeref.getFullName());
          if (part == null) {
            scan(value, typeref.getRef(), found);
          } else if (value instanceof String) {
            addIfCustom(
                (String) value, part, lastPartTyperefs.contains(typeref.getFullName()), found);
          }
          break;
        case RECORD:
          scanRecord(value, (RecordDataSchema) schema, found);
          break;
        case ARRAY:
          DataSchema items = ((ArrayDataSchema) schema).getItems();
          if (value instanceof DataList && reaches(items)) {
            for (Object element : (DataList) value) {
              scan(element, items, found);
            }
          }
          break;
        case MAP:
          DataSchema values = ((MapDataSchema) schema).getValues();
          if (value instanceof DataMap && reaches(values)) {
            for (Object element : ((DataMap) value).values()) {
              scan(element, values, found);
            }
          }
          break;
        case UNION:
          if (value instanceof DataMap) {
            UnionDataSchema union = (UnionDataSchema) schema;
            ((DataMap) value)
                .forEach(
                    (memberKey, member) ->
                        scan(member, union.getTypeByMemberKey(memberKey), found));
          }
          break;
        default:
          break;
      }
    }

    private void scanRecord(
        @Nullable Object value,
        @Nonnull RecordDataSchema record,
        @Nonnull Map<String, String> found) {
      List<String> fieldNames = referencingFields.get(record.getFullName());
      if (fieldNames == null || !(value instanceof DataMap)) {
        return;
      }
      DataMap map = (DataMap) value;
      for (String fieldName : fieldNames) {
        Object child = map.get(fieldName);
        if (child != null) {
          scan(child, record.getField(fieldName).getType(), found);
        }
      }
    }

    private boolean reaches(@Nullable DataSchema schema) {
      return reachesUrnWithEnv(schema, envPartByTyperef.keySet(), referencingFields.keySet());
    }

    private void addIfCustom(
        @Nonnull String urn, int part, boolean lastPart, @Nonnull Map<String, String> found) {
      if (lastPart && urn.endsWith(")")) {
        int comma = urn.lastIndexOf(',');
        if (comma > 0 && FabricTypeNames.isBuiltIn(urn.substring(comma + 1, urn.length() - 1))) {
          return;
        }
      }
      try {
        List<String> parts = Urn.createFromString(urn).getEntityKey().getParts();
        if (part < parts.size() && !FabricTypeNames.isBuiltIn(parts.get(part))) {
          found.put(urn, parts.get(part));
        }
      } catch (URISyntaxException e) {
        // Rejected by the URN validators.
      }
    }
  }

  @Nonnull
  public static UrnReferences discoverReferences(@Nonnull EntityRegistry entityRegistry) {
    List<AspectPluginConfig.EntityAspectName> aspects = new ArrayList<>();
    Map<String, Integer> envPartByTyperef = new TreeMap<>();
    Set<String> lastPartTyperefs = new TreeSet<>();
    Map<String, RecordDataSchema> records = new TreeMap<>();
    for (EntitySpec entitySpec : entityRegistry.getEntitySpecs().values()) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        Map<String, Integer> found = new TreeMap<>();
        walk(
            aspectSpec.getPegasusSchema(),
            new HashSet<>(),
            node -> {
              if (node.getType() == DataSchema.Type.TYPEREF) {
                TyperefDataSchema typeref = (TyperefDataSchema) node;
                envPart(typeref)
                    .ifPresent(
                        part -> {
                          found.put(typeref.getFullName(), part);
                          if (isLastPart(typeref, part)) {
                            lastPartTyperefs.add(typeref.getFullName());
                          }
                        });
              } else if (node.getType() == DataSchema.Type.RECORD) {
                records.put(((RecordDataSchema) node).getFullName(), (RecordDataSchema) node);
              }
            });
        if (!found.isEmpty()) {
          aspects.add(
              AspectPluginConfig.EntityAspectName.builder()
                  .entityName(entitySpec.getName())
                  .aspectName(aspectSpec.getName())
                  .build());
          envPartByTyperef.putAll(found);
        }
      }
    }
    aspects.sort(
        Comparator.comparing(AspectPluginConfig.EntityAspectName::getEntityName)
            .thenComparing(AspectPluginConfig.EntityAspectName::getAspectName));
    return new UrnReferences(
        aspects,
        envPartByTyperef,
        lastPartTyperefs,
        referencingFields(records.values(), envPartByTyperef.keySet()));
  }

  /**
   * Finds the records that reach a typed URN with an environment through their fields, to a fixed
   * point since a record may refer to itself.
   */
  @Nonnull
  private static Map<String, List<String>> referencingFields(
      @Nonnull Collection<RecordDataSchema> records, @Nonnull Set<String> urnTyperefs) {
    Set<String> referencing = new HashSet<>();
    boolean changed = true;
    while (changed) {
      changed = false;
      for (RecordDataSchema record : records) {
        if (!referencing.contains(record.getFullName())
            && record.getFields().stream()
                .anyMatch(field -> reachesUrnWithEnv(field.getType(), urnTyperefs, referencing))) {
          referencing.add(record.getFullName());
          changed = true;
        }
      }
    }
    Map<String, List<String>> fieldsByRecord = new TreeMap<>();
    for (RecordDataSchema record : records) {
      if (referencing.contains(record.getFullName())) {
        fieldsByRecord.put(
            record.getFullName(),
            record.getFields().stream()
                .filter(field -> reachesUrnWithEnv(field.getType(), urnTyperefs, referencing))
                .map(RecordDataSchema.Field::getName)
                .collect(Collectors.toList()));
      }
    }
    return fieldsByRecord;
  }

  /**
   * Whether values of this schema can hold a typed URN with an environment: it is one, or it
   * contains a record known to reach one. Records are not entered; the caller tracks them.
   */
  private static boolean reachesUrnWithEnv(
      @Nullable DataSchema schema,
      @Nonnull Set<String> urnTyperefs,
      @Nonnull Set<String> referencingRecords) {
    if (schema == null) {
      return false;
    }
    switch (schema.getType()) {
      case TYPEREF:
        TyperefDataSchema typeref = (TyperefDataSchema) schema;
        return urnTyperefs.contains(typeref.getFullName())
            || reachesUrnWithEnv(typeref.getRef(), urnTyperefs, referencingRecords);
      case RECORD:
        return referencingRecords.contains(((RecordDataSchema) schema).getFullName());
      case ARRAY:
        return reachesUrnWithEnv(
            ((ArrayDataSchema) schema).getItems(), urnTyperefs, referencingRecords);
      case MAP:
        return reachesUrnWithEnv(
            ((MapDataSchema) schema).getValues(), urnTyperefs, referencingRecords);
      case UNION:
        return ((UnionDataSchema) schema)
            .getMembers().stream()
                .anyMatch(
                    member -> reachesUrnWithEnv(member.getType(), urnTyperefs, referencingRecords));
      default:
        return false;
    }
  }

  /** The position of the environment among a typed URN's parts, from its TypedUrnValidator. */
  @SuppressWarnings("unchecked")
  @Nonnull
  static Optional<Integer> envPart(@Nonnull TyperefDataSchema typeref) {
    Object validate = typeref.getProperties().get("validate");
    if (!(validate instanceof Map)) {
      return Optional.empty();
    }
    Object typedUrn = ((Map<String, Object>) validate).get(TYPED_URN_VALIDATOR);
    if (!(typedUrn instanceof Map)) {
      return Optional.empty();
    }
    Object parts = ((Map<String, Object>) typedUrn).get("fields");
    if (!(parts instanceof List)) {
      return Optional.empty();
    }
    List<Object> partList = (List<Object>) parts;
    for (int index = 0; index < partList.size(); index++) {
      Object part = partList.get(index);
      if (part instanceof Map
          && FabricTypeNames.TYPEREF_FULL_NAME.equals(((Map<String, Object>) part).get("type"))) {
        return Optional.of(index);
      }
    }
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  private static boolean isLastPart(@Nonnull TyperefDataSchema typeref, int part) {
    Map<String, Object> typedUrn =
        (Map<String, Object>)
            ((Map<String, Object>) typeref.getProperties().get("validate"))
                .get(TYPED_URN_VALIDATOR);
    return ((List<Object>) typedUrn.get("fields")).size() == part + 1;
  }

  /** The distinct (entity, aspect) pairs, for a plugin's supported entity aspect names. */
  @Nonnull
  public static List<AspectPluginConfig.EntityAspectName> entityAspectNames(
      @Nonnull List<Field> fields) {
    return fields.stream()
        .map(
            field ->
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName(field.getEntityName())
                    .aspectName(field.getAspectName())
                    .build())
        .distinct()
        .collect(Collectors.toList());
  }

  @Nullable
  private static String searchFieldName(@Nonnull AspectSpec aspectSpec, @Nonnull String field) {
    return aspectSpec.getSearchableFieldSpecs().stream()
        .filter(spec -> List.of(field).equals(spec.getPath().getPathComponents()))
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .map(annotation -> annotation.getFieldName())
        .findFirst()
        .orElse(null);
  }

  private static void rejectNested(
      @Nullable DataSchema schema, @Nonnull String where, @Nonnull Set<String> visited) {
    walk(
        schema,
        visited,
        node -> {
          if (FabricTypeNames.isFabricTypeName(node)) {
            throw new IllegalStateException(
                "FabricTypeName is only supported as a top-level aspect field, found one nested in "
                    + where);
          }
        });
  }

  /** Visits every schema reachable from {@code schema}, typerefs before what they refer to. */
  private static void walk(
      @Nullable DataSchema schema,
      @Nonnull Set<String> visited,
      @Nonnull Consumer<DataSchema> visitor) {
    if (schema == null) {
      return;
    }
    visitor.accept(schema);
    switch (schema.getType()) {
      case TYPEREF:
        walk(((TyperefDataSchema) schema).getRef(), visited, visitor);
        break;
      case RECORD:
        RecordDataSchema record = (RecordDataSchema) schema;
        if (visited.add(record.getFullName())) {
          record.getFields().forEach(field -> walk(field.getType(), visited, visitor));
        }
        break;
      case ARRAY:
        walk(((ArrayDataSchema) schema).getItems(), visited, visitor);
        break;
      case MAP:
        walk(((MapDataSchema) schema).getValues(), visited, visitor);
        break;
      case UNION:
        ((UnionDataSchema) schema)
            .getMembers()
            .forEach(member -> walk(member.getType(), visited, visitor));
        break;
      default:
        break;
    }
  }
}
