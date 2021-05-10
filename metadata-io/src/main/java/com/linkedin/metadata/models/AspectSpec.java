package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.AspectAnnotation;

import javax.annotation.Nonnull;
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
}


