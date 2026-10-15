package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.URN_SEARCH_ANALYZER;

import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Keyword query of the Search V3 entity indices. Full text reads the root unified fields from
 * {@link SearchableAnnotation#indexedName()}: {@code .text}, and {@code .stemmed} at half weight
 * except on {@code name}, {@code qualifiedName}, and {@code urn}. The weight is the annotation
 * {@code boostScore}, scaled by the partial factor. Exact matches read the keyword form.
 */
@Slf4j
public class V3SearchQueryBuilder extends SearchQueryBuilder {

  // A stemmed match is a looser one, so it counts for less than the same word unstemmed
  private static final float STEMMED_FACTOR = 0.5f;

  private static final Set<String> IDENTITY_TEXT_FIELDS =
      Set.of(textField(V3SearchFields.NAME), textField(V3SearchFields.QUALIFIED_NAME));

  private final PartialConfiguration partialConfiguration;
  private final CustomizedQueryHandler customizedQueryHandler;
  // Indexed names of each searched entity list, derived once from the same annotations that build
  // the mapping
  private final Map<List<String>, Map<String, List<String>>> queryFieldsByEntities =
      new ConcurrentHashMap<>();

  public V3SearchQueryBuilder(
      @Nonnull final SearchConfiguration searchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration) {
    super(searchConfiguration, customSearchConfiguration, true);
    if (searchConfiguration.getPartial() == null) {
      this.partialConfiguration = new PartialConfiguration();
      this.partialConfiguration.setFactor(0.4f);
      this.partialConfiguration.setUrnFactor(0.5f);
    } else {
      this.partialConfiguration = searchConfiguration.getPartial();
    }
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(searchConfiguration.getCustom(), customSearchConfiguration)
            .build();
  }

  /**
   * The shared full-text fields a query of these entities reads, after the request's field
   * configuration, each with the root fields that feed it, as the indices they are searched in map
   * them.
   */
  @Nonnull
  public Map<String, List<String>> searchedFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Map<String, List<String>> fields =
        queryFields(opContext.getEntityRegistry(), entitySpecs);
    final Map<String, List<String>> configured =
        configuredFields(
            fields,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault));
    return configured != null ? configured : fields;
  }

  /**
   * Analyzed shared fields selected for a request, with the same weights used by the non-template
   * Stage 1 query. Search templates use this rather than rebuilding V3 fields from PDL root names.
   */
  @Nonnull
  public List<SearchFieldConfig> searchedFieldConfigs(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Collection<EntitySpec> group =
        V3SearchFields.indexGroupSpecs(opContext.getEntityRegistry(), entitySpecs);
    return searchedFields(opContext, entitySpecs).keySet().stream()
        .flatMap(name -> queryConfigs(group, name).stream())
        .toList();
  }

  @Override
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    return withSharedFields(
        super.getStandardFields(entityRegistry, entitySpecs),
        queryFields(entityRegistry, entitySpecs),
        V3SearchFields.indexGroupSpecs(entityRegistry, entitySpecs));
  }

  @Override
  protected Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    return withSharedFields(
        super.getStandardFields(entityRegistry, entitySpec),
        queryFields(entityRegistry, List.of(entitySpec)),
        V3SearchFields.indexGroupSpecs(entityRegistry, List.of(entitySpec)));
  }

  /**
   * Keeps fields the configuration selects. A configured name matches an indexed field or a root
   * field that feeds one.
   */
  @Override
  protected Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchFieldConfig> fields,
      @Nullable String label) {
    final Map<String, List<String>> queryFields =
        queryFields(opContext.getEntityRegistry(), entitySpecs);
    final Map<String, List<String>> selected = configuredFields(queryFields, label);
    if (selected == null) {
      return fields;
    }
    final Map<String, Set<String>> destinations = new HashMap<>();
    queryFields.forEach(
        (name, roots) ->
            roots.forEach(
                root -> destinations.computeIfAbsent(root, key -> new HashSet<>()).add(name)));
    return fields.stream()
        .filter(
            field -> {
              final String name = baseName(field.fieldName());
              return selected.containsKey(name)
                  || destinations.getOrDefault(name, Set.of()).stream()
                      .anyMatch(selected::containsKey);
            })
        .collect(Collectors.toSet());
  }

  @Override
  protected List<String> fqnMatchFields() {
    return List.of(textField(V3SearchFields.QUALIFIED_NAME), textField(V3SearchFields.URN));
  }

  @Override
  protected String descriptionPhraseField() {
    return textField("description");
  }

  @Override
  protected boolean isDelimitedIdentityField(@Nonnull SearchFieldConfig cfg) {
    return IDENTITY_TEXT_FIELDS.contains(cfg.fieldName());
  }

  @Nonnull
  private Map<String, List<String>> queryFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    return queryFieldsByEntities.computeIfAbsent(
        entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList()),
        key ->
            V3SearchFields.queryFields(
                V3SearchFields.indexGroupSpecs(entityRegistry, entitySpecs)));
  }

  /**
   * Keyword root fields from the annotations, plus the analyzed subfields of unified full-text
   * fields. Word-gram and delimited subfields are not mapped on V3.
   */
  @Nonnull
  private Set<SearchFieldConfig> withSharedFields(
      @Nonnull final Set<SearchFieldConfig> standardFields,
      @Nonnull final Map<String, List<String>> queryFields,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchFieldConfig> fields =
        standardFields.stream()
            .filter(
                field ->
                    !field.isDelimitedSubfield()
                        && !field.isWordGramSubfield()
                        && !URN_SEARCH_ANALYZER.equals(field.analyzer()))
            .map(
                field ->
                    field.toBuilder()
                        .hasDelimitedSubfield(false)
                        .hasWordGramSubfields(false)
                        .build())
            .collect(Collectors.toCollection(HashSet::new));
    for (String name : queryFields.keySet()) {
      fields.addAll(queryConfigs(entitySpecs, name));
    }
    return fields;
  }

  @Nonnull
  private List<SearchFieldConfig> queryConfigs(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String name) {
    final float boost = V3SearchFields.boost(entitySpecs, name) * partialConfiguration.getFactor();
    if (!V3SearchFields.analyzedQueryField(entitySpecs, name)) {
      return List.of(keywordField(name, boost));
    }
    final List<SearchFieldConfig> configs = new ArrayList<>();
    configs.add(
        analyzedField(name, V3SearchFields.TEXT, V3SearchFields.TEXT_SEARCH_ANALYZER, boost));
    if (V3SearchFields.stems(name)) {
      configs.add(
          analyzedField(
              name,
              V3SearchFields.STEMMED,
              V3SearchFields.STEMMED_SEARCH_ANALYZER,
              boost * STEMMED_FACTOR));
    }
    return configs;
  }

  @Nonnull
  private static SearchFieldConfig analyzedField(
      @Nonnull final String name,
      @Nonnull final String subfield,
      @Nonnull final String analyzer,
      final float boost) {
    return SearchFieldConfig.builder()
        .fieldName(name + "." + subfield)
        .shortName(name)
        .isDelimitedSubfield(true)
        .boost(boost)
        .analyzer(analyzer)
        .isQueryByDefault(true)
        .build();
  }

  @Nonnull
  private static SearchFieldConfig keywordField(@Nonnull final String name, final float boost) {
    return SearchFieldConfig.builder()
        .fieldName(name)
        .shortName(name)
        .boost(boost)
        .isQueryByDefault(true)
        .build();
  }

  /**
   * The shared fields of {@code fields} that the field configuration {@code fieldConfigLabel}
   * selects, or null when none applies: no configuration, an invalid one, or one that selects
   * nothing.
   */
  @Nullable
  private Map<String, List<String>> configuredFields(
      @Nonnull final Map<String, List<String>> fields, @Nullable final String fieldConfigLabel) {
    final SearchFields searchFields = getSearchFields(fieldConfigLabel);
    if (searchFields == null) {
      return null;
    }
    if (!searchFields.isValid()) {
      log.error(
          "Invalid field configuration for label: {}. Replace cannot be used with add/remove.",
          fieldConfigLabel);
      return null;
    }
    if (!searchFields.getReplace().isEmpty()) {
      final Map<String, List<String>> replaced = named(fields, searchFields.getReplace());
      if (replaced.isEmpty()) {
        log.warn(
            "Field configuration replace resulted in no valid fields for label: {}. "
                + "Using base fields instead.",
            fieldConfigLabel);
        return null;
      }
      return replaced;
    }
    final Map<String, List<String>> result = new LinkedHashMap<>(fields);
    named(fields, searchFields.getRemove()).keySet().forEach(result::remove);
    // Only a removed shared field can be added back: a field that feeds none is not copied into
    // the shared fields, so no query can reach it
    named(fields, searchFields.getAdd()).forEach(result::put);
    if (result.isEmpty()) {
      // A query over no fields would fall back to the engine's default fields
      log.warn(
          "Field configuration removed every searchable field for label: {}. "
              + "Using base fields instead.",
          fieldConfigLabel);
      return null;
    }
    return result;
  }

  @Nullable
  private SearchFields getSearchFields(@Nullable final String fieldConfigLabel) {
    final CustomSearchConfiguration customSearchConfiguration =
        customizedQueryHandler.getCustomSearchConfiguration();
    if (fieldConfigLabel == null
        || customSearchConfiguration == null
        || customSearchConfiguration.getFieldConfigurations() == null) {
      return null;
    }
    final FieldConfiguration fieldConfiguration =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);
    return fieldConfiguration == null ? null : fieldConfiguration.getSearchFields();
  }

  /** A configured name selects an indexed field or a root field that feeds one. */
  @Nonnull
  private static Map<String, List<String>> named(
      @Nonnull final Map<String, List<String>> fields, @Nonnull final List<String> names) {
    final Set<String> baseNames =
        names.stream().map(V3SearchQueryBuilder::baseName).collect(Collectors.toSet());
    return fields.entrySet().stream()
        .filter(
            field ->
                baseNames.contains(field.getKey())
                    || field.getValue().stream().anyMatch(baseNames::contains))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (left, right) -> left, LinkedHashMap::new));
  }

  @Nonnull
  private static String baseName(@Nonnull final String configuredName) {
    final int subfield = configuredName.indexOf('.');
    return subfield > 0 ? configuredName.substring(0, subfield) : configuredName;
  }

  @Nonnull
  private static String textField(@Nonnull final String field) {
    return field + "." + V3SearchFields.TEXT;
  }
}
