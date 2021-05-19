package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectSpec {

  private final AspectAnnotation _aspectAnnotation;
  private final Map<PathSpec, SearchableFieldSpec> _searchableFieldSpecs;
  private final Map<PathSpec, RelationshipFieldSpec> _relationshipFieldSpecs;
  private final Map<PathSpec, BrowsePathFieldSpec> _browsePathFieldSpec;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _schema;

  public AspectSpec(@Nonnull final AspectAnnotation aspectAnnotation,
      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpec,
      @Nonnull final List<BrowsePathFieldSpec> browsePathFieldSpecs) {
    this(aspectAnnotation, searchableFieldSpecs, relationshipFieldSpec, browsePathFieldSpecs, null);
  }

  public AspectSpec(
      @Nonnull final AspectAnnotation aspectAnnotation,
      @Nonnull final List<SearchableFieldSpec> searchableFieldSpecs,
      @Nonnull final List<RelationshipFieldSpec> relationshipFieldSpec,
      @Nonnull final List<BrowsePathFieldSpec> browsePathFieldSpecs, final RecordDataSchema schema) {
    _aspectAnnotation = aspectAnnotation;
    _searchableFieldSpecs = searchableFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
    _relationshipFieldSpecs = relationshipFieldSpec.stream()
        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
    _browsePathFieldSpec = browsePathFieldSpecs.stream()
        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
    _schema = schema;
  }

  public String getName() {
    return _aspectAnnotation.getName();
  }

  public Boolean isKey() {
    return _aspectAnnotation.getIsKey();
  }

  public Map<PathSpec, SearchableFieldSpec> getSearchableFieldSpecMap() {
    return _searchableFieldSpecs;
  }

  public Map<PathSpec, RelationshipFieldSpec> getRelationshipFieldSpecMap() {
    return _relationshipFieldSpecs;
  }

  public Map<PathSpec, BrowsePathFieldSpec> getBrowsePathFieldSpecMap() {
    return _browsePathFieldSpec;
  }

  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return new ArrayList<>(_searchableFieldSpecs.values());
  }

  public List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
    return new ArrayList<>(_relationshipFieldSpecs.values());
  }

  public List<BrowsePathFieldSpec> getBrowsePathFieldSpecs() {
    return new ArrayList<>(_browsePathFieldSpec.values());
  }

  public RecordDataSchema getPegasusSchema() {
    return _schema;
  }
}


