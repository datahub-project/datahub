package com.linkedin.experimental;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntitySpec {

    private final String _name;
    private final Boolean _searchable;
    private final Boolean _browsable;
    private final Map<String, AspectSpec> _aspectSpecs;

    public EntitySpec(final String name,
                      final Boolean searchable,
                      final Boolean browsable,
                      final List<AspectSpec> aspectSpecs) {
        _name = name;
        _searchable = searchable;
        _browsable = browsable;
        _aspectSpecs = aspectSpecs != null ? aspectSpecs.stream().collect(Collectors.toMap(spec -> spec.getName(), spec -> spec)) : null;
    }

    public String getName() {
        return _name;
    }

    public Boolean isSearchable() {
        return _searchable;
    }

    public Boolean isBrowsable() {
        return _browsable;
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

