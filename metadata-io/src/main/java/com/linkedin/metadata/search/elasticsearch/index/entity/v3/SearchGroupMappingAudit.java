package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;

import com.linkedin.metadata.models.EntitySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Deterministic pre-build audit for one V3 search-group mapping. */
public class SearchGroupMappingAudit {
  private final String searchGroup;
  private final List<String> entityNames;
  private final int mappedFieldCount;
  private final Integer maxFieldsLimit;
  private final Map<String, Set<String>> fieldNameConflicts;
  private final Map<String, Set<String>> fieldNameAliasConflicts;
  private final Map<String, Set<String>> fieldNameTypeConflicts;
  private final Map<String, Set<String>> fieldNameAliasTypeConflicts;
  private final Map<String, Set<String>> nonResolvableFieldTypeConflicts;
  private final Map<String, Set<String>> nonResolvableFieldNameAliasTypeConflicts;

  private SearchGroupMappingAudit(
      @Nonnull final String searchGroup,
      @Nonnull final Collection<EntitySpec> entitySpecs,
      @Nonnull final Map<String, Object> mappings,
      @Nullable final Integer maxFieldsLimit,
      @Nonnull final ConflictResolver.ConflictResult conflictResult) {
    this.searchGroup = searchGroup;
    this.entityNames =
        Collections.unmodifiableList(
            entitySpecs.stream().map(EntitySpec::getName).sorted().collect(Collectors.toList()));
    this.mappedFieldCount = countMappedFields(mappings);
    this.maxFieldsLimit = maxFieldsLimit;
    this.fieldNameConflicts = immutableSortedCopy(conflictResult.getFieldNameConflicts());
    this.fieldNameAliasConflicts = immutableSortedCopy(conflictResult.getFieldNameAliasConflicts());
    this.fieldNameTypeConflicts = immutableSortedCopy(conflictResult.getFieldNameTypeConflicts());
    this.fieldNameAliasTypeConflicts =
        immutableSortedCopy(conflictResult.getFieldNameAliasTypeConflicts());
    this.nonResolvableFieldTypeConflicts =
        immutableSortedCopy(findNonResolvableTypeConflicts(fieldNameTypeConflicts));
    this.nonResolvableFieldNameAliasTypeConflicts =
        immutableSortedCopy(findNonResolvableTypeConflicts(fieldNameAliasTypeConflicts));
  }

  @Nonnull
  public static SearchGroupMappingAudit create(
      @Nonnull final String searchGroup,
      @Nonnull final Collection<EntitySpec> entitySpecs,
      @Nonnull final Map<String, Object> mappings,
      @Nullable final Integer maxFieldsLimit) {
    return new SearchGroupMappingAudit(
        searchGroup,
        entitySpecs,
        mappings,
        maxFieldsLimit,
        ConflictResolver.detectConflicts(entitySpecs));
  }

  @Nonnull
  public String getSearchGroup() {
    return searchGroup;
  }

  @Nonnull
  public List<String> getEntityNames() {
    return entityNames;
  }

  public int getEntityCount() {
    return entityNames.size();
  }

  public int getMappedFieldCount() {
    return mappedFieldCount;
  }

  @Nullable
  public Integer getMaxFieldsLimit() {
    return maxFieldsLimit;
  }

  public boolean isOverFieldLimit() {
    return maxFieldsLimit != null && mappedFieldCount > maxFieldsLimit;
  }

  public boolean hasNonResolvableTypeConflicts() {
    return !nonResolvableFieldTypeConflicts.isEmpty()
        || !nonResolvableFieldNameAliasTypeConflicts.isEmpty();
  }

  public boolean isPassing() {
    return !isOverFieldLimit() && !hasNonResolvableTypeConflicts();
  }

  @Nonnull
  public Map<String, Set<String>> getFieldNameConflicts() {
    return fieldNameConflicts;
  }

  @Nonnull
  public Map<String, Set<String>> getFieldNameAliasConflicts() {
    return fieldNameAliasConflicts;
  }

  @Nonnull
  public Map<String, Set<String>> getFieldNameTypeConflicts() {
    return fieldNameTypeConflicts;
  }

  @Nonnull
  public Map<String, Set<String>> getFieldNameAliasTypeConflicts() {
    return fieldNameAliasTypeConflicts;
  }

  @Nonnull
  public Map<String, Set<String>> getNonResolvableFieldTypeConflicts() {
    return nonResolvableFieldTypeConflicts;
  }

  @Nonnull
  public Map<String, Set<String>> getNonResolvableFieldNameAliasTypeConflicts() {
    return nonResolvableFieldNameAliasTypeConflicts;
  }

  @Nonnull
  public Map<String, Object> toMap() {
    final Map<String, Object> result = new TreeMap<>();
    result.put("searchGroup", searchGroup);
    result.put("entityCount", getEntityCount());
    result.put("entities", entityNames);
    result.put("mappedFieldCount", mappedFieldCount);
    result.put("maxFieldsLimit", maxFieldsLimit);
    result.put("overFieldLimit", isOverFieldLimit());
    result.put("passing", isPassing());
    result.put("fieldNameConflicts", fieldNameConflicts);
    result.put("fieldNameAliasConflicts", fieldNameAliasConflicts);
    result.put("fieldNameTypeConflicts", fieldNameTypeConflicts);
    result.put("fieldNameAliasTypeConflicts", fieldNameAliasTypeConflicts);
    result.put("nonResolvableFieldTypeConflicts", nonResolvableFieldTypeConflicts);
    result.put(
        "nonResolvableFieldNameAliasTypeConflicts", nonResolvableFieldNameAliasTypeConflicts);
    return Collections.unmodifiableMap(result);
  }

  @SuppressWarnings("unchecked")
  static int countMappedFields(@Nonnull final Map<String, Object> mappings) {
    final Object properties = mappings.get(PROPERTIES);
    if (!(properties instanceof Map)) {
      return 0;
    }
    return countProperties((Map<String, Object>) properties);
  }

  @SuppressWarnings("unchecked")
  private static int countProperties(@Nonnull final Map<String, Object> properties) {
    int count = 0;
    for (Object value : properties.values()) {
      // ES total_fields.limit counts object/nested nodes, so keep this pre-flight count aligned.
      count++;
      if (value instanceof Map) {
        final Object childProperties = ((Map<String, Object>) value).get(PROPERTIES);
        if (childProperties instanceof Map) {
          count += countProperties((Map<String, Object>) childProperties);
        }
        // Multifields under "fields" (.keyword/.delimited/.ngram ...) count toward
        // total_fields.limit too; skipping them lets the audit pass a mapping the engine rejects.
        final Object multiFields = ((Map<String, Object>) value).get(FIELDS);
        if (multiFields instanceof Map) {
          count += countProperties((Map<String, Object>) multiFields);
        }
      }
    }
    return count;
  }

  @Nonnull
  private static Map<String, Set<String>> findNonResolvableTypeConflicts(
      @Nonnull final Map<String, Set<String>> conflicts) {
    final Map<String, Set<String>> result = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : conflicts.entrySet()) {
      try {
        ConflictResolver.resolveTypeConflict(entry.getValue());
      } catch (IllegalArgumentException e) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  @Nonnull
  private static Map<String, Set<String>> immutableSortedCopy(
      @Nonnull final Map<String, Set<String>> source) {
    final Map<String, Set<String>> sorted = new TreeMap<>();
    source.forEach(
        (key, value) ->
            sorted.put(key, Collections.unmodifiableSet(new TreeSet<>(new ArrayList<>(value)))));
    return Collections.unmodifiableMap(sorted);
  }
}
