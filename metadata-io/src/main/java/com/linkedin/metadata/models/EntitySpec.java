package com.linkedin.metadata.models;

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

    public static class EntityAnnotation {
        private final String _name;
        private final Boolean _searchable;
        private final Boolean _browsable;

        public EntityAnnotation(@Nonnull final String name,
                                @Nonnull final Boolean searchable,
                                @Nonnull final Boolean browsable) {
            _name = name;
            _searchable = searchable;
            _browsable = browsable;
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

        public static EntityAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
            if (Map.class.isAssignableFrom(annotationObj.getClass())) {
                Map map = (Map) annotationObj;
                final Object nameObj = map.get("name");
                final Object searchableObj = map.get("searchable");
                final Object browsableObj = map.get("browsable");
                if (nameObj == null || !String.class.isAssignableFrom(nameObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required @Entity field 'name' field of type String");
                }
                if (searchableObj == null || !Boolean.class.isAssignableFrom(searchableObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required @Entity field 'searchable' field of type Boolean");
                }
                if (browsableObj == null || !Boolean.class.isAssignableFrom(browsableObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required @Entity field 'browsable' field of type Boolean");
                }
                return new EntityAnnotation((String) nameObj, (Boolean) searchableObj, (Boolean) browsableObj);
            }
            throw new IllegalArgumentException("Failed to validate @Entity annotation object: Invalid value type provided (Expected Map)");
        }
    }
}

