package com.linkedin.metadata.models;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** A specification of a DataHub Entity */
public interface EntitySpec {

  String ARRAY_WILDCARD = "*";

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

  /**
   * Gets the search group for this entity.
   *
   * @return the search group name
   */
  String getSearchGroup();

  default List<SearchableFieldSpec> getSearchableFieldSpecs() {
    return getAspectSpecs().stream()
        .map(AspectSpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  default Map<String, Set<SearchableAnnotation.FieldType>> getSearchableFieldTypes() {
    // Get additional fields and mint SearchableFieldSpecs for them
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>>
        numValuesFn =
            (aspectSpec, searchableFieldSpec) -> {
              String fieldName =
                  searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get();
              Set<SearchableAnnotation.FieldType> fieldTypes = new HashSet<>();
              fieldTypes.add(SearchableAnnotation.FieldType.COUNT);
              return new Pair<>(fieldName, fieldTypes);
            };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>>
        hasValuesFn =
            (aspectSpec, searchableFieldSpec) -> {
              String fieldName =
                  searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get();
              Set<SearchableAnnotation.FieldType> fieldTypes = new HashSet<>();
              fieldTypes.add(SearchableAnnotation.FieldType.BOOLEAN);
              return new Pair<>(fieldName, fieldTypes);
            };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, Set<SearchableAnnotation.FieldType>>>
        defaultKeyFn =
            (aspectSpec, searchableFieldSpec) -> {
              String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
              Set<SearchableAnnotation.FieldType> fieldTypes =
                  new HashSet<>(
                      Collections.singleton(
                          searchableFieldSpec.getSearchableAnnotation().getFieldType()));
              return new Pair<>(fieldName, fieldTypes);
            };
    BinaryOperator<Set<SearchableAnnotation.FieldType>> mergeFn =
        (set1, set2) -> {
          set1.addAll(set2);
          return set1;
        };
    return getFieldMap(numValuesFn, hasValuesFn, defaultKeyFn, mergeFn);
  }

  default <T, R> Map<T, R> getFieldMap(
      BiFunction<AspectSpec, SearchableFieldSpec, Pair<T, R>> numValuesFieldMapKeyFn,
      BiFunction<AspectSpec, SearchableFieldSpec, Pair<T, R>> hasValuesFieldMapKeyFn,
      BiFunction<AspectSpec, SearchableFieldSpec, Pair<T, R>> defaultMapKeyFn,
      BinaryOperator<R> mergeFunction) {
    Map<T, R> fieldSpecMap = new HashMap<>();
    for (AspectSpec aspectSpec : getAspectSpecs()) {
      for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
        SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();
        if (searchableAnnotation.getNumValuesFieldName().isPresent()) {
          Pair<T, R> numValuesKeyValue = numValuesFieldMapKeyFn.apply(aspectSpec, fieldSpec);
          fieldSpecMap.put(numValuesKeyValue.getKey(), numValuesKeyValue.getValue());
        }
        if (searchableAnnotation.getHasValuesFieldName().isPresent()) {
          Pair<T, R> hasValuesKeyValue = hasValuesFieldMapKeyFn.apply(aspectSpec, fieldSpec);
          fieldSpecMap.put(hasValuesKeyValue.getKey(), hasValuesKeyValue.getValue());
        }
      }
      fieldSpecMap.putAll(
          aspectSpec.getSearchableFieldSpecs().stream()
              .map(searchableField -> defaultMapKeyFn.apply(aspectSpec, searchableField))
              .collect(Collectors.toMap(Pair::getKey, Pair::getValue, mergeFunction)));
    }
    return fieldSpecMap;
  }

  /**
   * Creates a map of path specs to field names where the path is
   * aspectName/FIELD_PATH(/numValuesFieldName | /hasValuesFieldName)? (Regex)
   *
   * @return map of pathspecs to field names
   */
  default Map<PathSpec, String> getSearchableFieldPathMap() {
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<PathSpec, String>> numValuesFn =
        (aspectSpec, searchableFieldSpec) -> {
          List<String> fieldPaths = new ArrayList<>();
          fieldPaths.add(aspectSpec.getName());
          fieldPaths.addAll(searchableFieldSpec.getPath().getPathComponents());
          fieldPaths.add(
              searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get());
          PathSpec pathSpec =
              new PathSpec(
                  fieldPaths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          String fieldName =
              searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get();
          return new Pair<>(pathSpec, fieldName);
        };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<PathSpec, String>> hasValuesFn =
        (aspectSpec, searchableFieldSpec) -> {
          String fieldName =
              searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get();
          List<String> fieldPaths = new ArrayList<>();
          fieldPaths.add(aspectSpec.getName());
          fieldPaths.addAll(searchableFieldSpec.getPath().getPathComponents());
          fieldPaths.add(
              searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get());
          PathSpec pathSpec =
              new PathSpec(
                  fieldPaths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          return new Pair<>(pathSpec, fieldName);
        };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<PathSpec, String>> defaultKeyFn =
        (aspectSpec, searchableFieldSpec) -> {
          String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
          List<String> paths = new ArrayList<>();
          paths.add(aspectSpec.getName());
          paths.addAll(searchableFieldSpec.getPath().getPathComponents());
          PathSpec pathSpec =
              new PathSpec(
                  paths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          return new Pair<>(pathSpec, fieldName);
        };
    BinaryOperator<String> mergeFn =
        (s1, s2) -> {
          if (!StringUtils.equals(s1, s2)) {
            throw new IllegalStateException(
                String.format(
                    "Path must be unique within an entity, unable to merge values: %s and %s",
                    s1, s2));
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

  /**
   * Creates a map of searchable field names to path specs where the path is
   * aspectName/FIELD_PATH(/numValuesFieldName | /hasValuesFieldName)? (Regex)
   *
   * @return map of searchable field names to path specs
   */
  default Map<String, List<PathSpec>> getSearchableFieldsToPathSpecsMap() {
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, List<PathSpec>>> numValuesFn =
        (aspectSpec, searchableFieldSpec) -> {
          List<String> fieldPaths = new ArrayList<>();
          fieldPaths.add(aspectSpec.getName());
          fieldPaths.addAll(searchableFieldSpec.getPath().getPathComponents());
          fieldPaths.add(
              searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get());
          PathSpec pathSpec =
              new PathSpec(
                  fieldPaths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          String fieldName =
              searchableFieldSpec.getSearchableAnnotation().getNumValuesFieldName().get();
          return new Pair<>(fieldName, ImmutableList.of(pathSpec));
        };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, List<PathSpec>>> hasValuesFn =
        (aspectSpec, searchableFieldSpec) -> {
          String fieldName =
              searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get();
          List<String> fieldPaths = new ArrayList<>();
          fieldPaths.add(aspectSpec.getName());
          fieldPaths.addAll(searchableFieldSpec.getPath().getPathComponents());
          fieldPaths.add(
              searchableFieldSpec.getSearchableAnnotation().getHasValuesFieldName().get());
          PathSpec pathSpec =
              new PathSpec(
                  fieldPaths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          return new Pair<>(fieldName, ImmutableList.of(pathSpec));
        };
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, List<PathSpec>>> defaultKeyFn =
        (aspectSpec, searchableFieldSpec) -> {
          String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
          List<String> paths = new ArrayList<>();
          paths.add(aspectSpec.getName());
          paths.addAll(searchableFieldSpec.getPath().getPathComponents());
          PathSpec pathSpec =
              new PathSpec(
                  paths.stream()
                      .filter(str -> !ARRAY_WILDCARD.equals(str))
                      .collect(Collectors.toList()));
          return new Pair<>(fieldName, ImmutableList.of(pathSpec));
        };
    BinaryOperator<List<PathSpec>> mergeFn =
        (s1, s2) -> {
          s1.addAll(s2);
          return s1;
        };

    return getFieldMap(numValuesFn, hasValuesFn, defaultKeyFn, mergeFn);
  }
}
