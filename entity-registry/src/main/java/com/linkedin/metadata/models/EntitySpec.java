package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EntitySpec {

    private final EntityAnnotation _entityAnnotation;
    private final Map<String, AspectSpec> _aspectSpecs;

    // Classpath & Pegasus-specific: Temporary.
    private final RecordDataSchema _snapshotSchema;
    private final TyperefDataSchema _aspectTyperefSchema;

    public EntitySpec(@Nonnull final List<AspectSpec> aspectSpecs,
                      @Nonnull final EntityAnnotation entityAnnotation) {
        this(aspectSpecs, entityAnnotation, null, null);
    }

    public EntitySpec(@Nonnull final List<AspectSpec> aspectSpecs,
                      @Nonnull final EntityAnnotation entityAnnotation,
                      final RecordDataSchema snapshotSchema,
                      final TyperefDataSchema aspectTyperefSchema) {
        _aspectSpecs = aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, Function.identity()));
        _entityAnnotation = entityAnnotation;
        _snapshotSchema = snapshotSchema;
        _aspectTyperefSchema = aspectTyperefSchema;
    }

    public String getName() {
        return _entityAnnotation.getName();
    }

    public Boolean isSearchable() {
        return _entityAnnotation.isSearchable();
    }

    public Boolean isBrowsable() {
        return _entityAnnotation.isBrowsable();
    }

    public List<AspectSpec> getAspectSpecs() {
        return new ArrayList<>(_aspectSpecs.values());
    }

    public Map<String, AspectSpec> getAspectSpecMap() {
        return _aspectSpecs;
    }

    public AspectSpec getAspectSpec(final String name) {
        return _aspectSpecs.get(name);
    }

    public RecordDataSchema getSnapshotSchema() {
        return _snapshotSchema;
    }

    public TyperefDataSchema getAspectTyperefSchema() {
        return _aspectTyperefSchema;
    }

}
