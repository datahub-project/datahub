package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.ToString;

@ToString
public class ConfigEntitySpec implements EntitySpec {

  private final EntityAnnotation _entityAnnotation;
  private final Map<String, AspectSpec> _aspectSpecs;

  private List<SearchableFieldSpec> _searchableFieldSpecs;
  private Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypeMap;

  public ConfigEntitySpec(
      @Nonnull final String entityName,
      @Nonnull final String keyAspect,
      @Nonnull final Collection<AspectSpec> aspectSpecs) {
    _aspectSpecs =
        aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, Function.identity()));
    _entityAnnotation = new EntityAnnotation(entityName, keyAspect);
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
    throw new UnsupportedOperationException(
        "Failed to find Snapshot associated with Config-based Entity");
  }

  @Override
  public TyperefDataSchema getAspectTyperefSchema() {
    throw new UnsupportedOperationException(
        "Failed to find Typeref schema associated with Config-based Entity");
  }

  @Override
  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    if (_searchableFieldSpecs == null) {
      _searchableFieldSpecs = EntitySpec.super.getSearchableFieldSpecs();
    }

    return _searchableFieldSpecs;
  }

  @Override
  public Map<String, Set<SearchableAnnotation.FieldType>> getSearchableFieldTypes() {
    if (searchableFieldTypeMap == null) {
      searchableFieldTypeMap = EntitySpec.super.getSearchableFieldTypes();
    }

    return searchableFieldTypeMap;
  }
}
