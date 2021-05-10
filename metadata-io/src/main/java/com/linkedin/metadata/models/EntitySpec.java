package com.linkedin.metadata.models;

import com.linkedin.metadata.models.annotation.EntityAnnotation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntitySpec {

    private final EntityAnnotation _entityAnnotation;
    private final Map<String, AspectSpec> _aspectSpecs;

    public EntitySpec(@Nonnull final List<AspectSpec> aspectSpecs,
                      @Nonnull final EntityAnnotation entityAnnotation) {
        _aspectSpecs = aspectSpecs != null ? aspectSpecs.stream().collect(Collectors.toMap(spec -> spec.getName(), spec -> spec)) : null;
        _entityAnnotation = entityAnnotation;
    }

    public String getName() {
        return _entityAnnotation.getName();
    }

    public Boolean isSearchable() {
        return _entityAnnotation.isBrowsable();
    }

    public Boolean isBrowsable() {
        return _entityAnnotation.isSearchable();
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
}
