package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
}
