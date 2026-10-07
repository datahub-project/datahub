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
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import io.datahubproject.metadata.context.OperationContext;
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
 * Keyword query of the Search V3 entity indices: the Stage 1 query of {@link SearchQueryBuilder},
 * full and light, over the shared {@code _search} fields (see {@link V3SearchFields}) instead of
 * the analyzed subfields of every searchable field, which V3 indices do not have.
 *
 * <p>Its analyzed clauses read each shared field's {@code text} subfield, and its {@code stemmed}
 * subfield at half that weight. Each shared field weighs what {@link V3SearchFields#weight} says,
 * scaled by the partial factor; {@code @Searchable} boost scores do not weight these matches. The
 * shared fields that name the entity stand for the name and qualified name of the Stage 1 query, so
 * its phrase prefixes, wildcards and name-focused light query read them. Exact matches read the
 * keywords of the root fields and the urn, as on V2, and there are no word-gram clauses. A field
 * configuration names the shared fields to search, either directly or by a field that feeds one.
 */
@Slf4j
public class V3SearchQueryBuilder extends SearchQueryBuilder {

  // A stemmed match is a looser one, so it counts for less than the same word unstemmed
  private static final float STEMMED_FACTOR = 0.5f;

  // The Stage 1 field a shared field stands for, where its name differs. DataHub Cloud's query
  // reads structured property values in customFullTextSearchFields
  private static final Map<String, String> STAGE_1_NAMES =
      Map.of(
          V3SearchFields.ENTITY_NAME,
          "name",
          V3SearchFields.STRUCTURED_PROPERTIES,
          V2MappingsBuilder.CUSTOM_FULL_TEXT_SEARCH_FIELDS);

  private static final Set<String> IDENTITY_TEXT_FIELDS =
      Set.of(textField(V3SearchFields.ENTITY_NAME), textField(V3SearchFields.QUALIFIED_NAME));

  private final PartialConfiguration partialConfiguration;
  private final CustomizedQueryHandler customizedQueryHandler;
  // The shared fields of each searched entity list, derived once from the same annotations that
  // build the mapping
  private final Map<List<String>, Map<String, List<String>>> fullTextFieldsByEntities =
      new ConcurrentHashMap<>();

  public V3SearchQueryBuilder(
      @Nonnull final SearchConfiguration searchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration) {
    super(searchConfiguration, customSearchConfiguration, true);
    this.partialConfiguration = searchConfiguration.getPartial();
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
        fullTextFields(opContext.getEntityRegistry(), entitySpecs);
    final Map<String, List<String>> configured =
        configuredFields(
            fields,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault));
    return configured != null ? configured : fields;
  }

  @Override
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    return withSharedFields(
        super.getStandardFields(entityRegistry, entitySpecs),
        fullTextFields(entityRegistry, entitySpecs));
  }

  @Override
  protected Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    return withSharedFields(
        super.getStandardFields(entityRegistry, entitySpec),
        fullTextFields(entityRegistry, List.of(entitySpec)));
  }

  /**
   * Keeps the subfields of the shared fields the configuration selects, and the root fields that
   * feed one of those. A root field that feeds no listed shared field, such as the urn or a field
   * of a referenced entity, lands in {@code other}.
   */
  @Override
  protected Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchFieldConfig> fields,
      @Nullable String label) {
    final Map<String, List<String>> fullTextFields =
        fullTextFields(opContext.getEntityRegistry(), entitySpecs);
    final Map<String, List<String>> selected = configuredFields(fullTextFields, label);
    if (selected == null) {
      return fields;
    }
    final Map<String, Set<String>> destinations = new HashMap<>();
    fullTextFields.forEach(
        (name, roots) ->
            roots.forEach(
                root -> destinations.computeIfAbsent(root, k -> new HashSet<>()).add(name)));
    return fields.stream()
        .filter(
            field ->
                field.fieldName().startsWith(V3SearchFields.path(""))
                    ? selected.containsKey(baseName(field.fieldName()))
                    : destinations
                        .getOrDefault(field.fieldName(), Set.of(V3SearchFields.OTHER))
                        .stream()
                        .anyMatch(selected::containsKey))
        .collect(Collectors.toSet());
  }

  @Override
  protected List<String> fqnMatchFields() {
    // On V3 an id is matched analyzed only in other
    return List.of(textField(V3SearchFields.QUALIFIED_NAME), textField(V3SearchFields.OTHER));
  }

  @Override
  protected String descriptionPhraseField() {
    return textField(V3SearchFields.DESCRIPTION);
  }

  @Override
  protected boolean isDelimitedIdentityField(@Nonnull SearchFieldConfig cfg) {
    return IDENTITY_TEXT_FIELDS.contains(cfg.fieldName());
  }

  @Nonnull
  private Map<String, List<String>> fullTextFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    return fullTextFieldsByEntities.computeIfAbsent(
        entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList()),
        k ->
            V3SearchFields.fullTextFields(
                V3SearchFields.indexGroupSpecs(entityRegistry, entitySpecs)));
  }

  /**
   * The root fields of these standard fields that a V3 index searches, and the analyzed subfields
   * of its shared fields. A root field has no analyzed subfields on V3, and a URN root field holds
   * the whole urn: {@code _search.other} searches its parts.
   */
  @Nonnull
  private Set<SearchFieldConfig> withSharedFields(
      @Nonnull final Set<SearchFieldConfig> standardFields,
      @Nonnull final Map<String, List<String>> fullTextFields) {
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
    for (String name : fullTextFields.keySet()) {
      final float boost = V3SearchFields.weight(name) * partialConfiguration.getFactor();
      fields.add(
          sharedField(name, V3SearchFields.TEXT, V3SearchFields.TEXT_SEARCH_ANALYZER, boost));
      fields.add(
          sharedField(
              name,
              V3SearchFields.STEMMED,
              V3SearchFields.STEMMED_SEARCH_ANALYZER,
              boost * STEMMED_FACTOR));
    }
    return fields;
  }

  @Nonnull
  private static SearchFieldConfig sharedField(
      @Nonnull final String name,
      @Nonnull final String subfield,
      @Nonnull final String analyzer,
      final float boost) {
    return SearchFieldConfig.builder()
        .fieldName(V3SearchFields.path(name) + "." + subfield)
        // After the field name, which resets them: the Stage 1 query picks its name and analyzed
        // clauses by these
        .shortName(STAGE_1_NAMES.getOrDefault(name, name))
        .isDelimitedSubfield(true)
        .boost(boost)
        .analyzer(analyzer)
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

  /**
   * The shared fields that configured names select: a name is a shared field (with or without
   * {@code _search.}) or a field that feeds one, and a subfield or {@code .*} stands for its field,
   * as V2 field configurations write them. The catch-all {@code other} is only selected by its own
   * name: a long-tail field does not stand for every other field queried by default.
   */
  @Nonnull
  private static Map<String, List<String>> named(
      @Nonnull final Map<String, List<String>> fields, @Nonnull final List<String> names) {
    final Set<String> baseNames =
        names.stream().map(V3SearchQueryBuilder::baseName).collect(Collectors.toSet());
    return fields.entrySet().stream()
        .filter(
            field ->
                baseNames.contains(field.getKey())
                    || (!V3SearchFields.OTHER.equals(field.getKey())
                        && field.getValue().stream().anyMatch(baseNames::contains)))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  @Nonnull
  private static String baseName(@Nonnull final String configuredName) {
    final String prefix = V3SearchFields.path("");
    final String name =
        configuredName.startsWith(prefix)
            ? configuredName.substring(prefix.length())
            : configuredName;
    final int subfield = name.indexOf('.');
    return subfield > 0 ? name.substring(0, subfield) : name;
  }

  @Nonnull
  private static String textField(@Nonnull final String field) {
    return V3SearchFields.path(field) + "." + V3SearchFields.TEXT;
  }
}
