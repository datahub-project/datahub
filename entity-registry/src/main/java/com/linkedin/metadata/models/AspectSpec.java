package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectSpec {

  private final AspectAnnotation _aspectAnnotation;
  private final Map<String, SearchableFieldSpec> _searchableFieldSpecs;
  private final Map<String, RelationshipFieldSpec> _relationshipFieldSpecs;
  private final Map<String, TemporalStatFieldSpec> _temporalStatFieldSpecs;
  private final Map<String, TemporalStatCollectionFieldSpec> _temporalStatCollectionFieldSpecs;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _schema;

  public AspectSpec(@Nonnull final AspectAnnotation aspectAnnotation,
      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpecs,
      @Nonnull final List<TemporalStatFieldSpec> temporalStatFieldSpecs,
      @Nonnull final List<TemporalStatCollectionFieldSpec> temporalStatCollectionFieldSpecs,
      final RecordDataSchema schema) {
    _aspectAnnotation = aspectAnnotation;
    _searchableFieldSpecs = searchableFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _relationshipFieldSpecs = relationshipFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _temporalStatFieldSpecs = temporalStatFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _temporalStatCollectionFieldSpecs = temporalStatCollectionFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath().toString(), spec -> spec, (val1, val2) -> val1));
    _schema = schema;
  }

  public String getName() {
    return _aspectAnnotation.getName();
  }

  public boolean isTemporal() {
    return _aspectAnnotation.isTemporal();
  }

  public Map<String, SearchableFieldSpec> getSearchableFieldSpecMap() {
    return _searchableFieldSpecs;
  }

  public Map<String, RelationshipFieldSpec> getRelationshipFieldSpecMap() {
    return _relationshipFieldSpecs;
  }

  public Map<String, TemporalStatFieldSpec> getTemporalStatFieldSpecMap() {
    return _temporalStatFieldSpecs;
  }

  public Map<String, TemporalStatCollectionFieldSpec> getTemporalStatCollectionFieldSpecMap() {
    return _temporalStatCollectionFieldSpecs;
  }

  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return new ArrayList<>(_searchableFieldSpecs.values());
  }

  public List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
    return new ArrayList<>(_relationshipFieldSpecs.values());
  }

  public List<TemporalStatFieldSpec> getTemporalStatFieldSpecs() {
    return new ArrayList<>(_temporalStatFieldSpecs.values());
  }

  public List<TemporalStatCollectionFieldSpec> getTemporalStatCollectionFieldSpecs() {
    return new ArrayList<>(_temporalStatCollectionFieldSpecs.values());
  }

  public RecordDataSchema getPegasusSchema() {
    return _schema;
  }
}


