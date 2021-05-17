package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import com.linkedin.metadata.models.annotation.AspectAnnotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class AspectSpecFromHandlers {

  private final AspectAnnotation _aspectAnnotation;

  // Classpath & Pegasus-specific: Temporary.
  private final RecordDataSchema _schema;

  // Classpath & Pegasus-specific: Temporary.
  private final SchemaAnnotationProcessor.SchemaAnnotationProcessResult _annotationProcessResult;

//  private final Map<PathSpec, SearchableFieldSpec> _searchableFieldSpecs;
//  private final Map<PathSpec, RelationshipFieldSpec> _relationshipFieldSpecs;
//  private final Map<PathSpec, BrowsePathFieldSpec> _browsePathFieldSpec;

  public AspectSpecFromHandlers(@Nonnull final AspectAnnotation aspectAnnotation,
      final SchemaAnnotationProcessor.SchemaAnnotationProcessResult annotationProcessResult
  ) {
    this(aspectAnnotation, annotationProcessResult, null);
  }

  public AspectSpecFromHandlers(@Nonnull final AspectAnnotation aspectAnnotation,
      final SchemaAnnotationProcessor.SchemaAnnotationProcessResult annotationProcessResult,
      final RecordDataSchema schema) {
    _aspectAnnotation = aspectAnnotation;
    _schema = schema;
    _annotationProcessResult = annotationProcessResult;
//    _searchableFieldSpecs = annotationProcessResult.getResultSchema();
//        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
//    _relationshipFieldSpecs = relationshipFieldSpec.stream()
//        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
//    _browsePathFieldSpec = browsePathFieldSpecs.stream()
//        .collect(Collectors.toMap(spec -> spec.getPath(), spec -> spec, (val1, val2) -> val1));
  }

  public String getName() {
    return _aspectAnnotation.getName();
  }

  public Boolean isKey() {
    return _aspectAnnotation.getIsKey();
  }

  public SchemaAnnotationProcessor.SchemaAnnotationProcessResult getAnnotationProcessResult() {
    return _annotationProcessResult;
  }

  public RecordDataSchema getPegasusSchema() {
    return _schema;
  }

  public List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return new ArrayList<>();
  }
}
