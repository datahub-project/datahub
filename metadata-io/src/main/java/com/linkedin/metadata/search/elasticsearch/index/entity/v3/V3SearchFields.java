package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDS;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_ANALYZER;
import static com.linkedin.metadata.search.utils.ESUtils.IGNORE_ABOVE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.TEXT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * Root unified fields of a Search V3 index. {@link SearchableAnnotation#indexedName()} is the field
 * a query names. A source whose own name differs copies into that field. {@code name} and {@code
 * qualifiedName} are analyzed without a stemmed subfield. The document {@code urn} keeps an
 * analyzed subfield for its parts. Dates, numbers, and keywords stay typed.
 */
public final class V3SearchFields {

  public static final String NAME = "name";
  public static final String QUALIFIED_NAME = "qualifiedName";
  public static final String URN = "urn";
  public static final String COLUMNS = "columns";

  public static final String TEXT = "text";
  public static final String STEMMED = "stemmed";

  public static final String TEXT_ANALYZER = "v3_text";
  public static final String TEXT_SEARCH_ANALYZER = "v3_text_search";
  public static final String STEMMED_ANALYZER = "v3_stemmed";
  public static final String STEMMED_SEARCH_ANALYZER = "v3_stemmed_search";

  public static final String WORD_TOKENIZER = "v3_word_tokenizer";

  private static final Set<String> UNSTEMMED = Set.of(NAME, QUALIFIED_NAME, URN);

  private static final int KEYWORD_IGNORE_ABOVE =
      ESUtils.keywordIgnoreAboveForMaxBytes(ESUtils.KEYWORD_MAXLENGTH);

  private static final Set<FieldType> STRING_FIELD_TYPES =
      Set.of(
          FieldType.KEYWORD,
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

  private static final Set<FieldType> FULL_TEXT_TYPES =
      Set.of(FieldType.TEXT, FieldType.TEXT_PARTIAL, FieldType.WORD_GRAM);

  private V3SearchFields() {}

  public static boolean isStringFieldType(@Nonnull final FieldType fieldType) {
    return STRING_FIELD_TYPES.contains(fieldType);
  }

  /** Full-text strings, plus the document urn, whose parts are searched on {@code urn.text}. */
  public static boolean isAnalyzed(@Nonnull final SearchableAnnotation annotation) {
    if (FULL_TEXT_TYPES.contains(annotation.getFieldType())) {
      return true;
    }
    return URN.equals(annotation.getFieldName())
        && (annotation.getFieldType() == FieldType.URN
            || annotation.getFieldType() == FieldType.URN_PARTIAL);
  }

  public static boolean stems(@Nonnull final String indexedName) {
    return !UNSTEMMED.contains(baseName(indexedName));
  }

  @Nonnull
  public static String baseName(@Nonnull final String indexedName) {
    final int underscore = indexedName.lastIndexOf('_');
    if (underscore > 0) {
      final String suffix = indexedName.substring(underscore + 1);
      if (!suffix.isEmpty() && suffix.chars().allMatch(Character::isDigit)) {
        return indexedName.substring(0, underscore);
      }
    }
    return indexedName;
  }

  /**
   * Indexed names full-text and keyword search reads for these entities, each with the root fields
   * that hold the value. A string field queried by default is included. {@code urn} always is.
   */
  @Nonnull
  public static Map<String, List<String>> queryFields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Map<String, LinkedHashSet<String>> sources = new LinkedHashMap<>();
    sourceFieldSpecs(entitySpecs)
        .forEach(
            fieldSpec -> {
              final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
              if (!annotation.isQueryByDefault() || !isStringFieldType(annotation.getFieldType())) {
                return;
              }
              sources
                  .computeIfAbsent(annotation.indexedName(), key -> new LinkedHashSet<>())
                  .add(annotation.getFieldName());
            });
    sources.computeIfAbsent(URN, key -> new LinkedHashSet<>()).add(URN);
    final Map<String, List<String>> fields = new LinkedHashMap<>();
    sources.forEach((name, roots) -> fields.put(name, List.copyOf(roots)));
    return fields;
  }

  /** Default weight of an indexed name: the shared {@code boostScore}, or 1. */
  public static float boost(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String indexedName) {
    return (float)
        sourceFieldSpecs(entitySpecs)
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .filter(annotation -> indexedName.equals(annotation.indexedName()))
            .map(SearchableAnnotation::getBoostScore)
            .findFirst()
            .map(Double::floatValue)
            .orElse(1f);
  }

  public static boolean analyzedQueryField(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String indexedName) {
    if (URN.equals(baseName(indexedName))) {
      return true;
    }
    return sourceFieldSpecs(entitySpecs)
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .anyMatch(
            annotation -> indexedName.equals(annotation.indexedName()) && isAnalyzed(annotation));
  }

  /**
   * Root fields whose values matched-fields can show. Column arrays are omitted: a table with
   * thousands of fields would be returned with every hit.
   */
  @Nonnull
  public static List<String> matchedFieldSources(
      @Nonnull final Map<String, List<String>> searchedFields) {
    return searchedFields.entrySet().stream()
        .filter(field -> !COLUMNS.equals(baseName(field.getKey())))
        .flatMap(field -> field.getValue().stream())
        .distinct()
        .collect(Collectors.toList());
  }

  @Nonnull
  public static List<EntitySpec> indexGroupSpecs(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<EntitySpec> groups = new LinkedHashSet<>();
    for (EntitySpec entitySpec : entitySpecs) {
      final Collection<EntitySpec> group =
          V3IndexKeys.entitySpecsForKey(entityRegistry, V3IndexKeys.resolve(entitySpec));
      groups.addAll(group.isEmpty() ? List.of(entitySpec) : group);
    }
    return List.copyOf(groups);
  }

  /**
   * Whether the target mapping analyzes a root field the current mapping does not. Full-text search
   * reads those subfields, so an index without them matches nothing.
   */
  public static boolean lacksSharedSearchFields(
      @Nonnull final Map<String, Object> currentMappings,
      @Nonnull final Map<String, Object> targetMappings) {
    final Map<?, ?> current = properties(currentMappings);
    return properties(targetMappings).entrySet().stream()
        .anyMatch(
            field ->
                isAnalyzedMapping(field.getValue())
                    && !isAnalyzedMapping(current.get(field.getKey())));
  }

  /** Keyword mapping that also carries the analyzed subfields of {@code indexedName}. */
  @Nonnull
  public static Map<String, Object> fullTextMapping(@Nonnull final String indexedName) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    mapping.put(IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE);
    mapping.put(FIELDS, new HashMap<>(Map.of(KEYWORD, keywordSubfield())));
    addAnalyzedSubfields(mapping, indexedName);
    return mapping;
  }

  /** Adds {@code .text} and, except on name fields and {@code urn}, {@code .stemmed}. */
  @SuppressWarnings("unchecked")
  public static void addAnalyzedSubfields(
      @Nonnull final Map<String, Object> mapping, @Nonnull final String indexedName) {
    final Map<String, Object> subfields =
        mapping.get(FIELDS) instanceof Map<?, ?> existing
            ? new HashMap<>((Map<String, Object>) existing)
            : new HashMap<>();
    subfields.putIfAbsent(TEXT, textMapping(TEXT_ANALYZER, TEXT_SEARCH_ANALYZER));
    if (stems(indexedName)) {
      subfields.putIfAbsent(STEMMED, textMapping(STEMMED_ANALYZER, STEMMED_SEARCH_ANALYZER));
    }
    mapping.put(FIELDS, subfields);
  }

  @Nonnull
  private static Map<String, Object> keywordSubfield() {
    return Map.of(TYPE, KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE);
  }

  @Nonnull
  private static Map<String, Object> textMapping(
      @Nonnull final String analyzer, @Nonnull final String searchAnalyzer) {
    return Map.of(TYPE, TEXT_FIELD_TYPE, ANALYZER, analyzer, SEARCH_ANALYZER, searchAnalyzer);
  }

  @Nonnull
  private static Map<?, ?> properties(@Nonnull final Map<String, Object> mappings) {
    return mappings.get("properties") instanceof Map<?, ?> properties ? properties : Map.of();
  }

  private static boolean isAnalyzedMapping(final Object mapping) {
    return mapping instanceof Map<?, ?> fieldMapping
        && fieldMapping.get(FIELDS) instanceof Map<?, ?> subfields
        && subfields.containsKey(TEXT);
  }

  @Nonnull
  private static Stream<SearchableFieldSpec> sourceFieldSpecs(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return entitySpecs.stream().flatMap(V3SearchFields::sourceFieldSpecs);
  }

  @Nonnull
  private static Stream<SearchableFieldSpec> sourceFieldSpecs(
      @Nonnull final EntitySpec entitySpec) {
    return entitySpec.getAspectSpecs().stream()
        .filter(aspectSpec -> !STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()))
        .map(AspectSpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .filter(
            spec -> !OBJECT_FIELD_TYPES.contains(spec.getSearchableAnnotation().getFieldType()));
  }
}
