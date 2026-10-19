package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DOC_VALUES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_ANALYZER;
import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
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
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The shared search fields of a Search V3 entity index. Full-text search reads root fields named by
 * {@code searchLabel}, not a {@code _search} object:
 *
 * <ul>
 *   <li>a searchable field copies into the shared field its {@code searchLabel} or {@code
 *       entityFieldName} names. A label is the semantic bucket, and tags applied from any aspect
 *       are {@code tags}. Names are {@code name} on Search V3;
 *   <li>a string field queried by default that names none copies into the root field {@code other},
 *       so V3 still searches every field V2 searches. An entity whose fields name no {@code name}
 *       sends the field its {@code _entityName} alias names to {@code name} instead. The urn and
 *       fields of an entity a reference field names ({@code @SearchableRef}) only ever copy into
 *       {@code other}, and matched fields do not report them.
 * </ul>
 *
 * <p>Structured properties are not a shared field. A {@code string}, {@code rich_text} or {@code
 * urn} property whose definition has {@code searchConfiguration} carries {@code text} and {@code
 * stemmed} subfields on its root keyword field (see {@link StructuredPropertyMappingBuilder}).
 * Search reads those subfields. {@code queryByDefault} decides whether an unqualified query
 * includes them. {@code copyTo} also copies each value into the named shared fields, so a query on
 * those fields matches the property. The stored document is unchanged.
 *
 * <p>These shared fields are the only full-text fields. Root fields and the {@code _aspects} copies
 * stay keywords and typed values for filters, facets and sorts, except the browse path v2 root,
 * which keeps its path analyzer. Browse path v1 is not indexed on Search V3. A shared field is a
 * full-text field when at least one string field queried by default feeds it in the index;
 * otherwise it stays a plain keyword, as the dates and other labels do.
 *
 * <p>Several root fields feeding one shared field are a union: every value is searchable, rebuilt
 * from the whole document on each write. When two aspects write the same root field, the value that
 * wins at the root (the user-edited aspect within a batch, the last write across batches) is the
 * one copied.
 */
public final class V3SearchFields {

  public static final String ENTITY_NAME = "name";
  public static final String QUALIFIED_NAME = "qualifiedName";
  public static final String DESCRIPTION = "description";
  public static final String COLUMNS = "columns";
  public static final String OTHER = "other";
  public static final String STRUCTURED_PROPERTIES = "structuredProperties";

  /** Word subfield of every full-text shared field. */
  public static final String TEXT = FieldMapping.TEXT;

  /** Stemmed subfield of every full-text shared field. */
  public static final String STEMMED = FieldMapping.STEMMED;

  public static final String TEXT_ANALYZER = "v3_text";
  public static final String TEXT_SEARCH_ANALYZER = "v3_text_search";
  public static final String STEMMED_ANALYZER = "v3_stemmed";
  public static final String STEMMED_SEARCH_ANALYZER = "v3_stemmed_search";

  /** Word tokenizer of those analyzers, unless a main tokenizer is configured. */
  public static final String WORD_TOKENIZER = "v3_word_tokenizer";

  private static final String ENTITY_NAME_ALIAS = "_entityName";

  /**
   * Root fields that hold one value per schema field. Matched fields never fetch them: a wide table
   * would be returned whole with every hit. Their search labels still decide which shared field is
   * searched.
   */
  public static final Set<String> COLUMN_ARRAY_FIELDS =
      Set.of(
          "fieldPaths",
          "fieldLabels",
          "fieldDescriptions",
          "editedFieldDescriptions",
          "fieldTags",
          "editedFieldTags",
          "fieldGlossaryTerms",
          "editedFieldGlossaryTerms");

  // The fields of other that matched fields report: those that name or tag the entity. The rest of
  // other, such as a document's text, custom properties or lists of urns, has no size bound
  static final Set<String> MATCHED_OTHER_FIELDS =
      Set.of("name", "editedName", "displayName", "fullName", "title", "tags", "glossaryTerms");

  // Shared fields that name the entity. Query weights use {@link #WEIGHTS}; the mapping shape comes
  // from the label's resolved root, not from this set.
  private static final Set<String> IDENTITY_FIELDS = Set.of(ENTITY_NAME, QUALIFIED_NAME);

  // Built-in destinations with no PDL field. Their keyword root is not an exact field; full text
  // reads the analyzed subfields. Model labels do not use this set. Structured properties are not
  // a shared field.
  private static final Set<String> SEARCH_ONLY_BUILTINS = Set.of(OTHER);

  // Indexed shared keywords skip longer values, whose UTF-8 bytes could exceed the engine's term
  // limit: copy_to copies a value the source field skipped
  private static final int KEYWORD_IGNORE_ABOVE =
      ESUtils.keywordIgnoreAboveForMaxBytes(ESUtils.KEYWORD_MAXLENGTH);

  /**
   * The {@code hasValuesFieldName} declared by a field that copies into {@code label}. Every source
   * of the label writes that boolean. Absent when the label declares none.
   */
  @Nonnull
  public static Optional<String> hasValuesFieldName(
      @Nonnull EntitySpec entitySpec, @Nonnull String label) {
    return entitySpec.getAspectSpecs().stream()
        .flatMap(aspect -> aspect.getSearchableFieldSpecs().stream())
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .map(SearchableAnnotation::resolvedForV3)
        .filter(SearchableAnnotation::isV3Searchable)
        .filter(annotation -> label.equals(annotation.getSearchLabel().orElse(null)))
        .map(SearchableAnnotation::getHasValuesFieldName)
        .flatMap(Optional::stream)
        .findFirst();
  }

  // Relative weight of a match in each full-text shared field; a field not listed weighs 1. These
  // replace the per-field @Searchable boostScore on V3. A structured property weighs what its own
  // searchConfiguration says.
  private static final Map<String, Float> WEIGHTS =
      Map.of(ENTITY_NAME, 10.0f, QUALIFIED_NAME, 10.0f, OTHER, 0.5f);

  private static final Set<FieldType> STRING_FIELD_TYPES =
      Set.of(
          FieldType.KEYWORD,
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

  private V3SearchFields() {}

  /** Where a shared field is indexed and queried. Labels are root fields. */
  @Nonnull
  public static String path(@Nonnull final String name) {
    return name;
  }

  public static boolean isIdentity(@Nonnull final String name) {
    return IDENTITY_FIELDS.contains(name);
  }

  public static float weight(@Nonnull final String name) {
    return WEIGHTS.getOrDefault(name, 1.0f);
  }

  /** Whether a field of this type holds strings, the only values the shared fields copy. */
  public static boolean isStringFieldType(@Nonnull final FieldType fieldType) {
    return STRING_FIELD_TYPES.contains(fieldType);
  }

  /**
   * The shared fields the root field of these source fields copies into. A label on any source wins
   * over the fallback to {@code other}, so a root field never lands in both.
   */
  @Nonnull
  public static Set<String> destinations(
      @Nonnull final Collection<SearchableFieldSpec> sourceFieldSpecs,
      @Nonnull final Set<SearchableFieldSpec> entityNameFallbacks) {
    final Set<String> destinations = new LinkedHashSet<>();
    sourceFieldSpecs.forEach(fieldSpec -> destinations.addAll(labels(fieldSpec)));
    if (destinations.isEmpty()) {
      sourceFieldSpecs.stream()
          .filter(V3SearchFields::isQueriedByDefault)
          .map(fieldSpec -> entityNameFallbacks.contains(fieldSpec) ? ENTITY_NAME : OTHER)
          .forEach(destinations::add);
    }
    return destinations;
  }

  /**
   * For entities none of whose fields name {@code name}, the fields their {@code _entityName} alias
   * names. They feed {@code name} instead of {@code other}, so every entity's name is in the same
   * shared field.
   */
  @Nonnull
  public static Set<SearchableFieldSpec> entityNameFallbacks(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .filter(
            entitySpec ->
                sourceFieldSpecs(entitySpec).noneMatch(spec -> labels(spec).contains(ENTITY_NAME)))
        .flatMap(V3SearchFields::sourceFieldSpecs)
        .filter(
            spec -> {
              final List<String> aliases = spec.getSearchableAnnotation().getFieldNameAliases();
              return aliases != null && aliases.contains(ENTITY_NAME_ALIAS);
            })
        .collect(Collectors.toSet());
  }

  /**
   * The shared fields full-text search reads for these entities, each with the root fields that
   * feed it, in the order the entities and their aspects declare them. A label is listed when a
   * contributor declares a {@code text} subfield and a string field queried by default feeds it.
   * {@code other} is listed, since every document's urn copies into it. Structured properties are
   * not listed: search reads the root field of each opted-in property.
   */
  @Nonnull
  public static Map<String, List<String>> fullTextFields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Map<String, Set<String>> sources = new LinkedHashMap<>();
    final Set<String> queried = new LinkedHashSet<>();
    rootSourceFieldSpecs(entitySpecs)
        .forEach(
            (rootField, fieldSpecs) -> {
              final boolean queriedByDefault =
                  declaresText(fieldSpecs)
                      && fieldSpecs.stream().anyMatch(V3SearchFields::isQueriedByDefault);
              for (String destination : destinations(fieldSpecs, fallbacks)) {
                sources.computeIfAbsent(destination, k -> new LinkedHashSet<>()).add(rootField);
                if (queriedByDefault) {
                  queried.add(destination);
                }
              }
            });
    // The catch-all goes last, so a hit's matched fields list the specific fields first
    queried.remove(OTHER);
    queried.add(OTHER);
    final Map<String, List<String>> fields = new LinkedHashMap<>();
    for (String name : queried) {
      fields.put(name, List.copyOf(sources.getOrDefault(name, Set.of())));
    }
    return fields;
  }

  /**
   * The root fields of the shared fields a search reads whose values matched fields are found from:
   * not the columns' arrays, and of {@code other} only the fields that name or tag the entity,
   * which "Matched on" shows. A fetch cannot cut a value short, so a table with thousands of
   * fields, a long document or a long list of urns would be sent whole with every hit; search still
   * reads every shared field.
   */
  @Nonnull
  public static List<String> matchedFieldSources(
      @Nonnull final Map<String, List<String>> searchedFields) {
    return searchedFields.entrySet().stream()
        .filter(field -> !COLUMNS.equals(field.getKey()))
        .flatMap(
            field ->
                OTHER.equals(field.getKey())
                    ? field.getValue().stream().filter(MATCHED_OTHER_FIELDS::contains)
                    : field.getValue().stream())
        .filter(source -> !COLUMN_ARRAY_FIELDS.contains(source))
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * The source fields of each root field, by root field name: a root field holds every searchable
   * field of that name, whichever aspect declares it. The mappings builder copies each root field
   * into the shared fields of all of them, so the mapping and the query agree.
   */
  @Nonnull
  public static Map<String, List<SearchableFieldSpec>> rootSourceFieldSpecs(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .flatMap(V3SearchFields::sourceFieldSpecs)
        .collect(
            Collectors.groupingBy(
                spec -> spec.getSearchableAnnotation().getFieldName(),
                LinkedHashMap::new,
                Collectors.toList()));
  }

  /**
   * These entities and every entity that shares a V3 index with one of them ({@code searchGroup}).
   * The mapping routes a root field by every entity of its index, so a label one entity puts on a
   * field also routes the fields of that name of the others; a search reads what the mapping wrote.
   */
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
   * Shared fields a structured property's {@code copyTo} writes into for these entities. An entry
   * names a shared field ({@code entityName}) or a root field that feeds one ({@code name}). A name
   * that is neither is skipped. Only a field full-text search reads is kept.
   */
  @Nonnull
  public static List<String> copyDestinations(
      @Nonnull final Collection<EntitySpec> entitySpecs,
      @Nullable final Collection<String> configured) {
    if (configured == null || configured.isEmpty()) {
      return List.of();
    }
    final Set<String> fullText = fullTextFields(entitySpecs).keySet();
    final Map<String, List<SearchableFieldSpec>> roots = rootSourceFieldSpecs(entitySpecs);
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Set<String> destinations = new LinkedHashSet<>();
    for (String entry : configured) {
      if (entry == null || entry.isBlank()) {
        continue;
      }
      final String name =
          entry.startsWith("_search.") ? entry.substring("_search.".length()) : entry;
      if (fullText.contains(name)) {
        destinations.add(name);
        continue;
      }
      final List<SearchableFieldSpec> sources = roots.get(name);
      if (sources == null) {
        continue;
      }
      destinations(sources, fallbacks).stream()
          .filter(fullText::contains)
          .forEach(destinations::add);
    }
    return List.copyOf(destinations);
  }

  /** Whether a root field of these entities copies into the shared field {@code name}. */
  public static boolean isFed(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String name) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    return rootSourceFieldSpecs(entitySpecs).values().stream()
        .anyMatch(sources -> destinations(sources, fallbacks).contains(name));
  }

  /**
   * Whether the target mapping has an analyzed shared field, one with a {@code text} subfield, that
   * the current mapping lacks. Full-text search reads that subfield, so an index without it quietly
   * matches nothing.
   */
  public static boolean lacksSharedSearchFields(
      @Nonnull final Map<String, Object> currentMappings,
      @Nonnull final Map<String, Object> targetMappings) {
    final Map<?, ?> current = rootProperties(currentMappings);
    return rootProperties(targetMappings).entrySet().stream()
        .anyMatch(
            field -> isAnalyzed(field.getValue()) && !isAnalyzed(current.get(field.getKey())));
  }

  @Nonnull
  private static Map<?, ?> rootProperties(@Nonnull final Map<String, Object> mappings) {
    return mappings.get("properties") instanceof Map<?, ?> properties ? properties : Map.of();
  }

  private static boolean isAnalyzed(final Object mapping) {
    return mapping instanceof Map<?, ?> fieldMapping
        && fieldMapping.get(FIELDS) instanceof Map<?, ?> subfields
        && subfields.containsKey(TEXT);
  }

  /**
   * The mapping of a built-in shared field. {@code other} and {@code structuredProperties} are a
   * search-only keyword plus {@code text} and {@code stemmed}. Model labels are composed from
   * {@code v3.mappings} and do not use this method.
   */
  @Nonnull
  public static Map<String, Object> mapping(@Nonnull final String name, final boolean fullText) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    if (!fullText || !SEARCH_ONLY_BUILTINS.contains(name)) {
      mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
      mapping.put(IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE);
    } else {
      mapping.put(ESUtils.INDEX, false);
      mapping.put(DOC_VALUES, false);
    }
    if (!fullText) {
      return mapping;
    }
    final Map<String, Object> subfields = new HashMap<>();
    subfields.put(TEXT, textMapping(TEXT_ANALYZER, TEXT_SEARCH_ANALYZER));
    subfields.put(STEMMED, textMapping(STEMMED_ANALYZER, STEMMED_SEARCH_ANALYZER));
    if (!SEARCH_ONLY_BUILTINS.contains(name)) {
      subfields.put(KEYWORD, Map.of(TYPE, KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE));
    }
    mapping.put(FIELDS, subfields);
    return mapping;
  }

  /**
   * {@code text} and {@code stemmed} subfields for a keyword field full-text search reads, using
   * the V3 analyzers. The caller copies any existing subfields first: {@link FieldTypeMapper}'s
   * string mapping is an immutable map.
   */
  @Nonnull
  public static Map<String, Object> analyzedSubfields() {
    return Map.of(
        TEXT, textMapping(TEXT_ANALYZER, TEXT_SEARCH_ANALYZER),
        STEMMED, textMapping(STEMMED_ANALYZER, STEMMED_SEARCH_ANALYZER));
  }

  /**
   * Analyzer subfields declared for each shared field. Full-text search reads {@code text} and
   * {@code stemmed} only when the resolved label declares them. {@code other} always declares both.
   */
  @Nonnull
  public static Map<String, Set<String>> analyzerSubfields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Map<String, Set<String>> subfields = new LinkedHashMap<>();
    rootSourceFieldSpecs(entitySpecs)
        .forEach(
            (rootField, fieldSpecs) -> {
              final Set<String> declared = declaredSubfields(fieldSpecs);
              if (declared.isEmpty()) {
                return;
              }
              for (String destination : destinations(fieldSpecs, fallbacks)) {
                subfields
                    .computeIfAbsent(destination, key -> new LinkedHashSet<>())
                    .addAll(declared);
              }
            });
    subfields.put(OTHER, Set.of(TEXT, STEMMED));
    return subfields;
  }

  private static boolean declaresText(@Nonnull final Collection<SearchableFieldSpec> fieldSpecs) {
    return declaredSubfields(fieldSpecs).contains(TEXT);
  }

  @Nonnull
  private static Set<String> declaredSubfields(
      @Nonnull final Collection<SearchableFieldSpec> fieldSpecs) {
    return fieldSpecs.stream()
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .filter(SearchableAnnotation::isV3Searchable)
        .map(SearchableAnnotation::resolvedForV3)
        .flatMap(annotation -> annotation.getV3Mappings().stream())
        .filter(mapping -> !mapping.isRoot())
        .map(mapping -> mapping.getSubfield())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static Map<String, Object> textMapping(
      @Nonnull final String analyzer, @Nonnull final String searchAnalyzer) {
    return Map.of(TYPE, TEXT_FIELD_TYPE, ANALYZER, analyzer, SEARCH_ANALYZER, searchAnalyzer);
  }

  private static Set<String> labels(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    if (!annotation.isV3Searchable()) {
      return Set.of();
    }
    final SearchableAnnotation resolved = annotation.resolvedForV3();
    return Stream.of(resolved.getSearchLabel(), resolved.getEntityFieldName())
        .flatMap(Optional::stream)
        .filter(label -> !label.isEmpty())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static boolean isQueriedByDefault(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
    return annotation.isV3Searchable()
        && annotation.isQueryByDefault()
        && isStringFieldType(annotation.getFieldType());
  }

  /**
   * Searchable fields projected to the root: object fields have no root field, and structured
   * properties keep their own fields.
   */
  private static Stream<SearchableFieldSpec> sourceFieldSpecs(
      @Nonnull final EntitySpec entitySpec) {
    return entitySpec.getAspectSpecs().stream()
        .filter(aspectSpec -> !STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()))
        .map(AspectSpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .filter(
            spec ->
                spec.getSearchableAnnotation().isV3Searchable()
                    && !OBJECT_FIELD_TYPES.contains(spec.getSearchableAnnotation().getFieldType()));
  }
}
