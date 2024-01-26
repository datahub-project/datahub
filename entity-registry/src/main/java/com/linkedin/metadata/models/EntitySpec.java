package com.linkedin.metadata.models;

import com.linkedin.data.schema.BooleanDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.LongDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A specification of a DataHub Entity */
public interface EntitySpec {
  String getName();

  EntityAnnotation getEntityAnnotation();

  String getKeyAspectName();

  AspectSpec getKeyAspectSpec();

  List<AspectSpec> getAspectSpecs();

  Map<String, AspectSpec> getAspectSpecMap();

  Boolean hasAspect(String name);

  AspectSpec getAspectSpec(String name);

  RecordDataSchema getSnapshotSchema();

  TyperefDataSchema getAspectTyperefSchema();

  default List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return getAspectSpecs().stream()
        .map(AspectSpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  default Map<String, Set<SearchableFieldSpec>> getSearchableFieldSpecMap() {
    // Get additional fields and mint SearchableFieldSpecs for them
    Map<String, Set<SearchableFieldSpec>> fieldSpecMap = new HashMap<>();
    for (SearchableFieldSpec fieldSpec : getSearchableFieldSpecs()) {
      SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();
      if (searchableAnnotation.getNumValuesFieldName().isPresent()) {
        String fieldName = searchableAnnotation.getNumValuesFieldName().get();
        SearchableFieldSpec numValuesField = getSearchableFieldSpec(fieldName, SearchableAnnotation.FieldType.COUNT,
            new LongDataSchema());
        Set<SearchableFieldSpec> fieldSet = new HashSet<>();
        fieldSet.add(numValuesField);
        fieldSpecMap.put(fieldName, fieldSet);
      }
      if (searchableAnnotation.getHasValuesFieldName().isPresent()) {
        String fieldName = searchableAnnotation.getHasValuesFieldName().get();
        SearchableFieldSpec hasValuesField = getSearchableFieldSpec(fieldName, SearchableAnnotation.FieldType.BOOLEAN,
            new BooleanDataSchema());
        Set<SearchableFieldSpec> fieldSet = new HashSet<>();
        fieldSet.add(hasValuesField);
        fieldSpecMap.put(fieldName, fieldSet);
      }
    }
    fieldSpecMap.putAll(getSearchableFieldSpecs().stream()
        .collect(
            Collectors.toMap(
                searchableFieldSpec -> searchableFieldSpec.getSearchableAnnotation().getFieldName(),
                searchableFieldSpec -> new HashSet<>(Collections.singleton(searchableFieldSpec)),
                (set1, set2) -> {
                  set1.addAll(set2);
                  return set1;
                })));
    return fieldSpecMap;
  }

  private static SearchableFieldSpec getSearchableFieldSpec(String fieldName, SearchableAnnotation.FieldType fieldType,
      DataSchema dataSchema) {
    SearchableAnnotation searchableAnnotation =
        new SearchableAnnotation(fieldName, fieldType, false,
            false, false, false, Optional.empty(),
            Optional.empty(), 1.0, Optional.empty(),
            Optional.empty(), Collections.emptyMap(), Collections.emptyList(),
            false);
    PathSpec pathSpec = new PathSpec(fieldName);
    return new SearchableFieldSpec(pathSpec, searchableAnnotation, dataSchema);
  }

  default List<SearchScoreFieldSpec> getSearchScoreFieldSpecs() {
    return getAspectSpecs().stream()
        .map(AspectSpec::getSearchScoreFieldSpecs)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  default List<RelationshipFieldSpec> getRelationshipFieldSpecs() {
    return getAspectSpecs().stream()
        .map(AspectSpec::getRelationshipFieldSpecs)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
