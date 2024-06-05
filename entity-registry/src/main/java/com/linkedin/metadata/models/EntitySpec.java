package com.linkedin.metadata.models;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;


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

  default Map<String, Set<SearchableAnnotation.FieldType>> getSearchableFieldTypes() {
    // Get additional fields and mint SearchableFieldSpecs for them
    Function<SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>> numValuesFn = searchableFieldSpec -> {
      String fieldName = searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get();
      Set<SearchableAnnotation.FieldType> fieldTypes = new HashSet<>();
      fieldTypes.add(SearchableAnnotation.FieldType.COUNT);
      return new Pair<>(fieldName, fieldTypes);
    };
    Function<SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>> hasValuesFn = searchableFieldSpec -> {
      String fieldName = searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get();
      Set<SearchableAnnotation.FieldType> fieldTypes = new HashSet<>();
      fieldTypes.add(SearchableAnnotation.FieldType.BOOLEAN);
      return new Pair<>(fieldName, fieldTypes);
    };
    Function<SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>> defaultKeyFn = searchableFieldSpec -> {
        String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
        Set<SearchableAnnotation.FieldType> fieldTypes = new HashSet<>(
            Collections.singleton(searchableFieldSpec.getSearchableAnnotation().getFieldType()));
        return new Pair<>(fieldName, fieldTypes);
    };
    BinaryOperator<Set<SearchableAnnotation.FieldType>> mergeFn =
        (set1, set2) -> {
          set1.addAll(set2);
          return set1;
        };
    return getFieldMap(numValuesFn, hasValuesFn, defaultKeyFn, mergeFn);
  }

  default <T,R> Map<T, R> getFieldMap(
      Function<SearchableFieldSpec, Pair<T, R>> numValuesFieldMapKeyFn,
      Function<SearchableFieldSpec, Pair<T, R>> hasValuesFieldMapKeyFn,
      Function<SearchableFieldSpec, Pair<T, R>> defaultMapKeyFn,
      BinaryOperator<R> mergeFunction) {
    Map<T, R> fieldSpecMap = new HashMap<>();
    for (SearchableFieldSpec fieldSpec : getSearchableFieldSpecs()) {
      SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();
      if (searchableAnnotation.getNumValuesFieldName().isPresent()) {
        Pair<T, R> numValuesKeyValue = numValuesFieldMapKeyFn.apply(fieldSpec);
        fieldSpecMap.put(numValuesKeyValue.getKey(), numValuesKeyValue.getValue());
      }
      if (searchableAnnotation.getHasValuesFieldName().isPresent()) {
        Pair<T, R> hasValuesKeyValue = hasValuesFieldMapKeyFn.apply(fieldSpec);
        fieldSpecMap.put(hasValuesKeyValue.getKey(), hasValuesKeyValue.getValue());
      }
    }
    fieldSpecMap.putAll(
        getSearchableFieldSpecs().stream()
            .map(defaultMapKeyFn)
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue, mergeFunction)));
    return fieldSpecMap;
  }

  default Map<PathSpec, String> getSearchableFieldPathMap() {
    Function<SearchableFieldSpec, Pair<PathSpec, String>> numValuesFn = searchableFieldSpec -> {
      List<String> fieldPaths = searchableFieldSpec.getPath().getPathComponents();
      fieldPaths.set(Math.max(fieldPaths.size() - 1, 0), searchableFieldSpec.getSearchableAnnotation()
          .getNumValuesFieldName().get());
      PathSpec pathSpec = new PathSpec(fieldPaths);
      String fieldName = searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get();
      return new Pair<>(pathSpec, fieldName);
    };
    Function<SearchableFieldSpec, Pair<PathSpec, String>> hasValuesFn = searchableFieldSpec -> {
      String fieldName = searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get();
      List<String> fieldPaths = searchableFieldSpec.getPath().getPathComponents();
      fieldPaths.set(Math.max(fieldPaths.size() - 1, 0), searchableFieldSpec.getSearchableAnnotation()
          .getHasValuesFieldName().get());
      PathSpec pathSpec = new PathSpec(fieldPaths);
      return new Pair<>(pathSpec, fieldName);
    };
    Function<SearchableFieldSpec, Pair<PathSpec, String>> defaultKeyFn = searchableFieldSpec -> {
      String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
      PathSpec pathSpec = searchableFieldSpec.getPath();
      return new Pair<>(pathSpec, fieldName);
    };
    BinaryOperator<String> mergeFn =
        (s1, s2) -> {
          if (!StringUtils.equals(s1, s2)) {
            throw new IllegalStateException(
                String.format("Path must be unique with an entity, unable to merge values: %s and %s", s1, s2));
          }
          return s1;
        };
    return getFieldMap(numValuesFn, hasValuesFn, defaultKeyFn, mergeFn);
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

  default List<SearchableRefFieldSpec> getSearchableRefFieldSpecs() {
    return getAspectSpecs().stream()
        .map(AspectSpec::getSearchableRefFieldSpecs)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
