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
import lombok.ToString;

/**
 * A partially specified entity spec that can be used with a {@link
 * com.linkedin.metadata.models.registry.PatchEntityRegistry}. Specifically, it does not require the
 * following things compared to a {@link DefaultEntitySpec} - a key aspect - snapshot schemas for
 * the entity - typeref schemas for aspect
 */
@ToString
public class PartialEntitySpec implements EntitySpec {

  private final EntityAnnotation _entityAnnotation;
  private final Map<String, AspectSpec> _aspectSpecs;

  public PartialEntitySpec(
      @Nonnull final Collection<AspectSpec> aspectSpecs, final EntityAnnotation entityAnnotation) {
    _aspectSpecs =
        aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, Function.identity()));
    _entityAnnotation = entityAnnotation;
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
    if (_entityAnnotation.getKeyAspect() != null) {
      return _aspectSpecs.get(_entityAnnotation.getKeyAspect());
    } else {
      return null;
    }
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
    throw new UnsupportedOperationException("Partial entity specs do not contain snapshot schemas");
  }

  @Override
  public TyperefDataSchema getAspectTyperefSchema() {
    throw new UnsupportedOperationException(
        "Partial entity specs do not contain aspect typeref schemas");
  }
}
