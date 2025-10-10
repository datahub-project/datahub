package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.ToString;

@ToString
public class DefaultEntitySpec implements EntitySpec {

  private final EntityAnnotation _entityAnnotation;
  private final Map<String, AspectSpec> _aspectSpecs;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _snapshotSchema;
  private final TyperefDataSchema _aspectTyperefSchema;

  private List<SearchableFieldSpec> _searchableFieldSpecs;
  private Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypeMap;
  private List<SearchableRefFieldSpec> _searchableRefFieldSpecs;
  private Map<PathSpec, String> searchableFieldPathMap;
  private Map<String, List<PathSpec>> fieldPathToSearchableFieldMap;

  public DefaultEntitySpec(
      @Nonnull final Collection<AspectSpec> aspectSpecs,
      @Nonnull final EntityAnnotation entityAnnotation,
      @Nonnull final RecordDataSchema snapshotSchema,
      @Nullable final TyperefDataSchema aspectTyperefSchema) {
    _aspectSpecs =
        aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, Function.identity()));
    _entityAnnotation = entityAnnotation;
    _snapshotSchema = snapshotSchema;
    _aspectTyperefSchema = aspectTyperefSchema;
  }

  public DefaultEntitySpec(
      @Nonnull final List<AspectSpec> aspectSpecs,
      @Nonnull final EntityAnnotation entityAnnotation,
      @Nonnull final RecordDataSchema snapshotSchema) {
    this(aspectSpecs, entityAnnotation, snapshotSchema, null);
  }

  @Override
  public String getName() {
    return _entityAnnotation.getName();
  }

  @Override
  public EntityAnnotation getEntityAnnotation() {
    return _entityAnnotation;
  }

  @Override
  public String getKeyAspectName() {
    return _entityAnnotation.getKeyAspect();
  }

  @Override
  public AspectSpec getKeyAspectSpec() {
    return _aspectSpecs.get(_entityAnnotation.getKeyAspect());
  }

  @Override
  public List<AspectSpec> getAspectSpecs() {
    return new ArrayList<>(_aspectSpecs.values());
  }

  @Override
  public Map<String, AspectSpec> getAspectSpecMap() {
    return _aspectSpecs;
  }

  @Override
  public Boolean hasAspect(final String name) {
    return _aspectSpecs.containsKey(name);
  }

  @Override
  public AspectSpec getAspectSpec(final String name) {
    return _aspectSpecs.get(name);
  }

  @Override
  public RecordDataSchema getSnapshotSchema() {
    return _snapshotSchema;
  }

  @Override
  public TyperefDataSchema getAspectTyperefSchema() {
    return _aspectTyperefSchema;
  }

  @Override
  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    if (_searchableFieldSpecs == null) {
      _searchableFieldSpecs = EntitySpec.super.getSearchableFieldSpecs();
    }

    return _searchableFieldSpecs;
  }

  @Override
  public List<SearchableRefFieldSpec> getSearchableRefFieldSpecs() {
    if (_searchableRefFieldSpecs == null) {
      _searchableRefFieldSpecs = EntitySpec.super.getSearchableRefFieldSpecs();
    }
    return _searchableRefFieldSpecs;
  }

  @Override
  public Map<String, Set<SearchableAnnotation.FieldType>> getSearchableFieldTypes() {
    if (searchableFieldTypeMap == null) {
      searchableFieldTypeMap = EntitySpec.super.getSearchableFieldTypes();
    }

    return searchableFieldTypeMap;
  }

  @Override
  public Map<PathSpec, String> getSearchableFieldPathMap() {
    if (searchableFieldPathMap == null) {
      searchableFieldPathMap = EntitySpec.super.getSearchableFieldPathMap();
    }

    return searchableFieldPathMap;
  }

  @Override
  public Map<String, List<PathSpec>> getSearchableFieldsToPathSpecsMap() {
    if (fieldPathToSearchableFieldMap == null) {
      fieldPathToSearchableFieldMap = EntitySpec.super.getSearchableFieldsToPathSpecsMap();
    }

    return fieldPathToSearchableFieldMap;
  }

  @Override
  public String getSearchGroup() {
    return _entityAnnotation.getSearchGroup();
  }
}
