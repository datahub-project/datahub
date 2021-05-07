package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AspectSpec {
    private final AspectAnnotation _aspectAnnotation;
    private final Map<PathSpec, SearchableFieldSpec> _searchableFieldSpecs;
    private final Map<PathSpec, RelationshipFieldSpec> _relationshipFieldSpecs;
    private final RecordDataSchema _pegasusSchema;

    public AspectSpec(@Nonnull final AspectAnnotation aspectAnnotation,
                      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
                      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpec,
                      @Nonnull final RecordDataSchema pegasusSchema) {
        _aspectAnnotation = aspectAnnotation;
        _searchableFieldSpecs = searchableFieldSpecs.stream().collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec));
        _relationshipFieldSpecs = relationshipFieldSpec.stream().collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec));
        _pegasusSchema = pegasusSchema;
    }

    public String getName() {
        return _aspectAnnotation.getName();
    }

    public Boolean isKey() {
        return _aspectAnnotation.isKey();
    }

    public Map<PathSpec, SearchableFieldSpec> getSearchableFieldSpecMap() {
        return _searchableFieldSpecs;
    }

    public Map<PathSpec, RelationshipFieldSpec> getRelationshipFieldSpecMap() {
        return _relationshipFieldSpecs;
    }

    public List<SearchableFieldSpec> getSearchableFieldSpecs() {
        return new ArrayList<>(_searchableFieldSpecs.values());
    }

    public List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
        return new ArrayList<>(_relationshipFieldSpecs.values());
    }

    public RecordDataSchema getPegasusSchema() {
        return _pegasusSchema;
    }

    public static class AspectAnnotation {

        private final String _name;
        private final Boolean _isKey;

        public AspectAnnotation(@Nonnull final String name,
                                @Nullable final Boolean isKey) {
            _name = name;
            _isKey = isKey != null && isKey;
        }

        public String getName() {
            return _name;
        }

        public Boolean isKey() {
            return _isKey;
        }

        public static AspectAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
            if (Map.class.isAssignableFrom(annotationObj.getClass())) {
                Map map = (Map) annotationObj;
                final Object nameObj = map.get("name");
                final Object isKeyObj = map.get("isKey");
                if (nameObj == null || !String.class.isAssignableFrom(nameObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required @Aspect field 'name' field of type String");
                }
                if (isKeyObj != null && !Boolean.class.isAssignableFrom(isKeyObj.getClass())) {
                    throw new IllegalArgumentException("Failed to validate required @Aspect field 'isKey' field of type Boolean");
                }
                return new AspectAnnotation((String) nameObj, (Boolean) isKeyObj);
            }
            throw new IllegalArgumentException("Failed to validate @Aspect annotation object: Invalid value type provided (Expected Map)");
        }
    }
}


