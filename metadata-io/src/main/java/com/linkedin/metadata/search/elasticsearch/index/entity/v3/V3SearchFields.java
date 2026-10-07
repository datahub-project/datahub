package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DOC_VALUES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDS;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.PARTIAL_ANALYZER;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * The shared search fields of a Search V3 entity index. Full-text search and autocomplete read a
 * few {@code _search.<name>} fields instead of every searchable field:
 *
 * <ul>
 *   <li>a searchable field copies into the shared field its {@code searchLabel} or {@code
 *       entityFieldName} names, for example {@code entityName} or {@code qualifiedName}. A field
 *       without either copies into the shared field {@link #DECLARED_FIELDS} lists for its search
 *       field name: one {@code description} for entity descriptions, and {@code columns} for the
 *       text of schema fields;
 *   <li>a string field queried by default that names none copies into {@code _search.other}, so V3
 *       still searches every field V2 searches. An entity whose fields name no {@code entityName}
 *       sends the field its {@code _entityName} alias names to {@code entityName} instead. The urn
 *       and fields of an entity a reference field names ({@code @SearchableRef}) only ever copy
 *       into {@code _search.other}, and matched fields do not report them;
 *   <li>a string field with {@code enableAutocomplete} also copies into {@code
 *       _search.autocomplete}, the only field with an ngram subfield;
 *   <li>a structured property's {@code string}, {@code rich_text} and {@code urn} values copy into
 *       {@code _search.structuredProperties} from the property's own field (see {@link
 *       StructuredPropertyMappingBuilder}), unless its definition opts out. No searchable field
 *       feeds it, so every index maps it and every search reads it.
 * </ul>
 *
 * <p>Only these shared fields are analyzed. Root fields and the {@code _aspects} copies stay
 * keywords and typed values for filters, facets and sorts. A shared field is a full-text field when
 * at least one string field queried by default feeds it in the index; otherwise it stays a plain
 * keyword, as the dates and other labels do.
 *
 * <p>Several root fields feeding one shared field are a union: every value is searchable, rebuilt
 * from the whole document on each write. When two aspects write the same root field, the value that
 * wins at the root (the user-edited aspect within a batch, the last write across batches) is the
 * one copied.
 */
public final class V3SearchFields {

  public static final String ENTITY_NAME = "entityName";
  public static final String QUALIFIED_NAME = "qualifiedName";
  public static final String DESCRIPTION = "description";
  public static final String COLUMNS = "columns";
  public static final String OTHER = "other";
  public static final String STRUCTURED_PROPERTIES = "structuredProperties";
  public static final String AUTOCOMPLETE = "autocomplete";

  /** Word subfield of every full-text shared field. */
  public static final String TEXT = "text";

  /** Stemmed subfield of every full-text shared field. */
  public static final String STEMMED = "stemmed";

  /** Search-as-you-type subfield of {@code _search.autocomplete}. */
  public static final String NGRAM = "ngram";

  public static final String TEXT_ANALYZER = "v3_text";
  public static final String TEXT_SEARCH_ANALYZER = "v3_text_search";
  public static final String STEMMED_ANALYZER = "v3_stemmed";
  public static final String STEMMED_SEARCH_ANALYZER = "v3_stemmed_search";

  /** Word tokenizer of those analyzers, unless a main tokenizer is configured. */
  public static final String WORD_TOKENIZER = "v3_word_tokenizer";

  private static final String ENTITY_NAME_ALIAS = "_entityName";

  // The shared field of searchable fields that carry no searchLabel, by search field name. A
  // searchLabel on the field takes precedence. Declared here rather than as labels in the models,
  // whose aspects would each need a new schema version for a search-only change
  static final Map<String, String> DECLARED_FIELDS =
      Map.ofEntries(
          Map.entry("description", DESCRIPTION),
          Map.entry("editedDescription", DESCRIPTION),
          Map.entry("definition", DESCRIPTION),
          Map.entry("assertionDescription", DESCRIPTION),
          Map.entry("fieldPaths", COLUMNS),
          Map.entry("fieldLabels", COLUMNS),
          Map.entry("fieldDescriptions", COLUMNS),
          Map.entry("editedFieldDescriptions", COLUMNS),
          Map.entry("fieldTags", COLUMNS),
          Map.entry("editedFieldTags", COLUMNS),
          Map.entry("fieldGlossaryTerms", COLUMNS),
          Map.entry("editedFieldGlossaryTerms", COLUMNS));

  // The fields of other that matched fields report: those that name or tag the entity. The rest of
  // other, such as a document's text, custom properties or lists of urns, has no size bound
  static final Set<String> MATCHED_OTHER_FIELDS =
      Set.of("name", "editedName", "displayName", "fullName", "title", "tags", "glossaryTerms");

  // Shared fields that name the entity keep an indexed keyword for exact match and sorting
  private static final Set<String> IDENTITY_FIELDS = Set.of(ENTITY_NAME, QUALIFIED_NAME);

  // Shared fields only full-text search reads, which hold just their analyzed subfields. A label a
  // model names keeps its indexed keyword for the sorts, filters and score functions it serves
  private static final Set<String> TEXT_ONLY_FIELDS =
      Set.of(DESCRIPTION, COLUMNS, STRUCTURED_PROPERTIES, OTHER);

  // Indexed shared keywords skip longer values, whose UTF-8 bytes could exceed the engine's term
  // limit: copy_to copies a value the source field skipped
  private static final int KEYWORD_IGNORE_ABOVE =
      ESUtils.keywordIgnoreAboveForMaxBytes(ESUtils.KEYWORD_MAXLENGTH);

  // Relative weight of a match in each full-text shared field; a field not listed weighs 1. These
  // replace the per-field @Searchable boostScore on V3. Structured property values weigh what
  // DataHub Cloud's query gives the field it copies them into
  private static final Map<String, Float> WEIGHTS =
      Map.of(ENTITY_NAME, 10.0f, QUALIFIED_NAME, 10.0f, STRUCTURED_PROPERTIES, 0.8f, OTHER, 0.5f);

  private static final Set<FieldType> STRING_FIELD_TYPES =
      Set.of(
          FieldType.KEYWORD,
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

  private V3SearchFields() {}

  /** The {@code _search} path of a shared field. */
  @Nonnull
  public static String path(@Nonnull final String name) {
    return MappingConstants.SEARCH_FIELD_NAME + "." + name;
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
    if (sourceFieldSpecs.stream().anyMatch(V3SearchFields::isAutocompleted)) {
      destinations.add(AUTOCOMPLETE);
    }
    return destinations;
  }

  /**
   * For entities none of whose fields name {@code entityName}, the fields their {@code _entityName}
   * alias names. They feed {@code entityName} instead of {@code other}, so every entity's name is
   * in the same shared field.
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
   * feed it, in the order the entities and their aspects declare them. A shared field is listed
   * when at least one string field queried by default feeds it; {@code structuredProperties} always
   * is, with no root field, since a property can be added to any entity, and {@code other} is,
   * since every document's urn copies into it.
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
                  fieldSpecs.stream().anyMatch(V3SearchFields::isQueriedByDefault);
              for (String destination : destinations(fieldSpecs, fallbacks)) {
                if (AUTOCOMPLETE.equals(destination)) {
                  continue;
                }
                sources.computeIfAbsent(destination, k -> new LinkedHashSet<>()).add(rootField);
                if (queriedByDefault) {
                  queried.add(destination);
                }
              }
            });
    queried.add(STRUCTURED_PROPERTIES);
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
        .filter(source -> !COLUMNS.equals(DECLARED_FIELDS.get(source)))
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

  /** Whether a root field of these entities copies into the shared field {@code name}. */
  public static boolean isFed(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String name) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    return rootSourceFieldSpecs(entitySpecs).values().stream()
        .anyMatch(sources -> destinations(sources, fallbacks).contains(name));
  }

  /**
   * Whether the target mapping has an analyzed shared field, one with a {@code text} or {@code
   * ngram} subfield, that the current mapping lacks. Full-text search and autocomplete read those
   * subfields, so an index without them quietly matches nothing; the mapping diff cannot show it,
   * since it leaves out the dynamic {@code _search} object.
   */
  public static boolean lacksSharedSearchFields(
      @Nonnull final Map<String, Object> currentMappings,
      @Nonnull final Map<String, Object> targetMappings) {
    final Map<?, ?> current = searchFields(currentMappings);
    return searchFields(targetMappings).entrySet().stream()
        .anyMatch(
            field -> isAnalyzed(field.getValue()) && !isAnalyzed(current.get(field.getKey())));
  }

  @Nonnull
  private static Map<?, ?> searchFields(@Nonnull final Map<String, Object> mappings) {
    return mappings.get("properties") instanceof Map<?, ?> properties
            && properties.get(MappingConstants.SEARCH_FIELD_NAME) instanceof Map<?, ?> search
            && search.get("properties") instanceof Map<?, ?> fields
        ? fields
        : Map.of();
  }

  private static boolean isAnalyzed(final Object mapping) {
    return mapping instanceof Map<?, ?> fieldMapping
        && fieldMapping.get(FIELDS) instanceof Map<?, ?> subfields
        && (subfields.containsKey(TEXT) || subfields.containsKey(NGRAM));
  }

  /**
   * The root fields that feed {@code _search.autocomplete}: first those copied into {@code
   * entityName}, then the rest, each group in declaration order.
   */
  @Nonnull
  public static List<String> autocompleteFields(@Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Map<Boolean, Set<String>> byName = new HashMap<>();
    entitySpecs.stream()
        .flatMap(V3SearchFields::sourceFieldSpecs)
        .filter(V3SearchFields::isAutocompleted)
        .forEach(
            fieldSpec ->
                byName
                    .computeIfAbsent(
                        destinations(List.of(fieldSpec), fallbacks).contains(ENTITY_NAME),
                        k -> new LinkedHashSet<>())
                    .add(fieldSpec.getSearchableAnnotation().getFieldName()));
    return Stream.concat(
            byName.getOrDefault(true, Set.of()).stream(),
            byName.getOrDefault(false, Set.of()).stream())
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * The mapping of a shared field. A full-text field gets {@code text} and {@code stemmed}
   * subfields; a field that names the entity also keeps an indexed normalized keyword for exact
   * match, sorting and name suggestions, with a {@code keyword} subfield that keeps the casing for
   * case-sensitive exact match, and the others only hold their subfields, unless a model names them
   * as a label. {@code autocomplete} is a normalized keyword with a search-as-you-type {@code
   * ngram} subfield, in the engine's shape.
   */
  @Nonnull
  public static Map<String, Object> mapping(
      @Nonnull final String name,
      final boolean fullText,
      @Nonnull final Map<String, String> partialNgramConfig) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    if (AUTOCOMPLETE.equals(name)) {
      final Map<String, Object> ngram = new HashMap<>(partialNgramConfig);
      ngram.put(ANALYZER, PARTIAL_ANALYZER);
      mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
      mapping.put(IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE);
      mapping.put(DOC_VALUES, false);
      mapping.put(FIELDS, Map.of(NGRAM, ngram));
      return mapping;
    }
    if (!fullText || !TEXT_ONLY_FIELDS.contains(name)) {
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
    if (isIdentity(name)) {
      subfields.put(KEYWORD, Map.of(TYPE, KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE));
    }
    mapping.put(FIELDS, subfields);
    return mapping;
  }

  private static Map<String, Object> textMapping(
      @Nonnull final String analyzer, @Nonnull final String searchAnalyzer) {
    return Map.of(TYPE, TEXT_FIELD_TYPE, ANALYZER, analyzer, SEARCH_ANALYZER, searchAnalyzer);
  }

  private static Set<String> labels(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    final Set<String> labels =
        Stream.of(annotation.getSearchLabel(), annotation.getEntityFieldName())
            .flatMap(Optional::stream)
            .filter(label -> !label.isEmpty())
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (labels.isEmpty() && DECLARED_FIELDS.containsKey(annotation.getFieldName())) {
      labels.add(DECLARED_FIELDS.get(annotation.getFieldName()));
    }
    return labels;
  }

  private static boolean isQueriedByDefault(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    return annotation.isQueryByDefault() && isStringFieldType(annotation.getFieldType());
  }

  private static boolean isAutocompleted(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    return annotation.isEnableAutocomplete() && isStringFieldType(annotation.getFieldType());
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
            spec -> !OBJECT_FIELD_TYPES.contains(spec.getSearchableAnnotation().getFieldType()));
  }
}
