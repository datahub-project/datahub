package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.v3.StructuredPropertyQueryFields;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.StructuredPropertyQuery;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.mockito.Mockito;

/** Loads the committed property catalog and the shipped query bodies the goldens render. */
public final class SearchTemplateFixtures {
  public static final String FIELD_EDITS = "urn:li:searchTemplate:fieldEdits";
  public static final String NAME_AND_VALUE = "urn:li:searchTemplate:nameAndValue";
  public static final String FULLTEXT_QUERY = "customer orders";
  public static final String NAME_VALUE_QUERY = "certification status gold";

  private static final YAMLMapper YAML = new YAMLMapper();
  private static final TypeReference<List<Map<String, Object>>> LIST = new TypeReference<>() {};

  private SearchTemplateFixtures() {}

  public record Property(
      @Nonnull String urn,
      @Nonnull String qualifiedName,
      @Nonnull String valueType,
      @Nonnull List<String> entityTypes,
      boolean queryByDefault,
      boolean fullTextSearch,
      double boostScore,
      @Nullable String displayName,
      @Nullable String version,
      @Nonnull String fieldPath,
      @Nonnull StructuredPropertyDefinition definition) {}

  public static final class Harness {
    public final Map<Urn, StructuredPropertyDefinition> definitions = new HashMap<>();
    public final Map<Urn, SearchTemplateProperties> stored = new HashMap<>();
    public final List<Property> properties = new ArrayList<>();
    public CachingAspectRetriever retriever;
    public OperationContext opContext;
    public SearchTemplateConfiguration templates;
    public SearchTemplateLoader loader;
    public EntityRegistry registry;

    public void setup() {
      definitions.clear();
      stored.clear();
      properties.clear();
      SearchTemplates.install(null);
      registry = TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();
      retriever = Mockito.mock(CachingAspectRetriever.class);
      Mockito.when(retriever.getEntityRegistry()).thenReturn(registry);
      Mockito.when(retriever.getLatestAspectObject(Mockito.any(), Mockito.any(), Mockito.any()))
          .thenAnswer(
              invocation -> {
                Urn urn = invocation.getArgument(1);
                String aspect = invocation.getArgument(2);
                if (STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(aspect)) {
                  StructuredPropertyDefinition definition = definitions.get(urn);
                  return definition == null ? null : new Aspect(definition.data().copy());
                }
                if (!SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME.equals(aspect)) {
                  return null;
                }
                SearchTemplateProperties template = stored.get(urn);
                return template == null ? null : new Aspect(template.data().copy());
              });
      Mockito.when(retriever.getLatestAspectObjects(Mockito.any(), Mockito.any(), Mockito.any()))
          .thenAnswer(
              invocation -> {
                Set<Urn> urns = invocation.getArgument(1);
                Set<String> aspects = invocation.getArgument(2);
                Map<Urn, Map<String, Aspect>> result = new HashMap<>();
                for (Urn urn : urns) {
                  Map<String, Aspect> byAspect = new HashMap<>();
                  if (aspects.contains(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)) {
                    StructuredPropertyDefinition definition = definitions.get(urn);
                    if (definition != null) {
                      byAspect.put(
                          STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                          new Aspect(definition.data().copy()));
                    }
                  }
                  result.put(urn, byAspect);
                }
                return result;
              });
      opContext =
          TestOperationContexts.systemContextNoSearchAuthorization(retriever)
              .withSearchFlags(flags -> flags.setFulltext(true));
      templates = new SearchTemplateConfiguration();
      templates.setEnabled(true);
      templates.setMaxLeafClauses(1024);
      templates
          .getDefaults()
          .put("fulltext", new HashMap<>(Map.of("default", SEARCH_TEMPLATE_FULLTEXT_URN)));
      templates
          .getDefaults()
          .put("structured", new HashMap<>(Map.of("default", "urn:li:searchTemplate:structured")));
      loader = new SearchTemplateLoader(600);
    }

    public void installCatalog() {
      for (Property property : readProperties()) {
        properties.add(property);
        definitions.put(UrnUtils.getUrn(property.urn()), property.definition());
      }
      StructuredPropertyQueryFields.replace(
          definitions.entrySet().stream()
              .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
              .toList());
    }

    public void installShippedTemplates() {
      readShipped().forEach(this::store);
      readChildren().forEach(this::store);
    }

    public void store(@Nonnull String urn, @Nonnull SearchTemplateProperties properties) {
      if (!properties.hasLastModified()) {
        properties.setLastModified(
            new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn(SYSTEM_ACTOR)));
      }
      stored.put(UrnUtils.getUrn(urn), properties);
      loader.invalidate(urn);
    }

    @Nonnull
    public ElasticSearchConfiguration elastic() {
      EntityIndexVersionConfiguration indexV3 = new EntityIndexVersionConfiguration();
      indexV3.setEnabled(true);
      indexV3.setKeywordReadEnabled(true);
      EntityIndexConfiguration index = new EntityIndexConfiguration();
      index.setV3(indexV3);
      SearchV3Configuration searchV3 = new SearchV3Configuration();
      searchV3.setTemplates(templates);
      SearchConfiguration search = new SearchConfiguration();
      search.setV3(searchV3);
      search.setMaxTermBucketSize(50);
      ElasticSearchConfiguration elastic = new ElasticSearchConfiguration();
      elastic.setEntityIndex(index);
      elastic.setSearch(search);
      return elastic;
    }

    @Nonnull
    public Property property(@Nonnull String qualifiedName) {
      return properties.stream()
          .filter(property -> property.qualifiedName().equals(qualifiedName))
          .findFirst()
          .orElseThrow();
    }
  }

  @Nonnull
  public static List<Property> readProperties() {
    List<Property> properties = new ArrayList<>();
    for (Map<String, Object> row :
        readList("/search/template/fixtures/structured-properties.yaml")) {
      properties.add(property(row));
    }
    return properties;
  }

  @Nonnull
  public static Map<String, SearchTemplateProperties> readShipped() {
    String yaml = readUtf8("/bootstrap_mcps/search-templates.yaml");
    yaml = yaml.replace("{{&auditStamp}}", "\"1970-01-01T00:00:00Z\"");
    List<Map<String, Object>> rows;
    try {
      rows = YAML.readValue(yaml, LIST);
    } catch (Exception e) {
      throw new IllegalStateException("Could not read shipped search templates", e);
    }
    Map<String, SearchTemplateProperties> byUrn = new LinkedHashMap<>();
    for (Map<String, Object> row : rows) {
      String urn = String.valueOf(row.get("entityUrn"));
      @SuppressWarnings("unchecked")
      Map<String, Object> aspect = (Map<String, Object>) row.get("aspect");
      byUrn.put(urn, shipped(aspect));
    }
    if (!byUrn.containsKey(SEARCH_TEMPLATE_FULLTEXT_URN)
        || !byUrn.containsKey("urn:li:searchTemplate:structured")) {
      throw new IllegalStateException("Shipped search templates were not found");
    }
    return byUrn;
  }

  @Nonnull
  static Map<String, SearchTemplateProperties> readChildren() {
    Map<String, SearchTemplateProperties> children = new LinkedHashMap<>();
    for (Map<String, Object> row : readList("/search/template/fixtures/template-children.yaml")) {
      children.put(String.valueOf(row.get("urn")), child(row));
    }
    return children;
  }

  @Nonnull
  private static Property property(@Nonnull Map<String, Object> row) {
    String urn = String.valueOf(row.get("urn"));
    String qualifiedName = String.valueOf(row.get("qualifiedName"));
    String valueType = String.valueOf(row.get("valueType"));
    @SuppressWarnings("unchecked")
    List<String> entityTypes =
        ((List<Object>) row.get("entityTypes")).stream().map(String::valueOf).toList();
    boolean queryByDefault = Boolean.TRUE.equals(row.get("queryByDefault"));
    boolean fullTextSearch = Boolean.TRUE.equals(row.get("fullTextSearch"));
    double boost =
        row.get("boostScore") == null ? 1.0 : ((Number) row.get("boostScore")).doubleValue();
    String displayName =
        row.get("displayName") == null ? null : String.valueOf(row.get("displayName"));
    String version = row.get("version") == null ? null : String.valueOf(row.get("version"));
    StructuredPropertyDefinition definition = new StructuredPropertyDefinition();
    definition.setQualifiedName(qualifiedName);
    definition.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub." + valueType));
    UrnArray types = new UrnArray();
    entityTypes.forEach(type -> types.add(UrnUtils.getUrn("urn:li:entityType:datahub." + type)));
    definition.setEntityTypes(types);
    if (displayName != null) {
      definition.setDisplayName(displayName);
    }
    if (version != null) {
      definition.setVersion(version);
    }
    if (row.containsKey("queryByDefault")
        || row.containsKey("boostScore")
        || row.containsKey("fullTextSearch")) {
      DataHubSearchConfig config = new DataHubSearchConfig();
      config.setQueryByDefault(queryByDefault);
      config.setBoostScore(boost);
      if (row.containsKey("fullTextSearch")) {
        config.setFullTextSearch(fullTextSearch);
      }
      definition.setSearchConfiguration(config);
    }
    Urn parsed = UrnUtils.getUrn(urn);
    String fieldPath =
        STRUCTURED_PROPERTY_MAPPING_FIELD + "." + toElasticsearchFieldName(parsed, definition);
    return new Property(
        urn,
        qualifiedName,
        valueType,
        entityTypes,
        queryByDefault,
        fullTextSearch,
        boost,
        displayName,
        version,
        fieldPath,
        definition);
  }

  @Nonnull
  private static SearchTemplateProperties shipped(@Nonnull Map<String, Object> aspect) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(String.valueOf(aspect.get("name")));
    properties.setDialect(SearchTemplateDialect.valueOf(String.valueOf(aspect.get("dialect"))));
    properties.setEditable(Boolean.TRUE.equals(aspect.get("editable")));
    properties.setQuery(unwrap(String.valueOf(aspect.get("query"))));
    if (aspect.get("functionScore") != null) {
      properties.setFunctionScore(unwrap(String.valueOf(aspect.get("functionScore"))));
    }
    return properties;
  }

  @Nonnull
  private static SearchTemplateProperties child(@Nonnull Map<String, Object> row) {
    SearchTemplateProperties properties = new SearchTemplateProperties();
    properties.setName(String.valueOf(row.get("name")));
    properties.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    properties.setEditable(true);
    properties.setParent(UrnUtils.getUrn(String.valueOf(row.get("parent"))));
    if (row.get("nameAndValueWeight") != null) {
      StructuredPropertyQuery section = new StructuredPropertyQuery();
      section.setNameAndValueWeight(((Number) row.get("nameAndValueWeight")).floatValue());
      properties.setStructuredPropertyQuery(section);
    }
    if (row.get("fields") instanceof Map<?, ?> fields) {
      SearchFieldOverrides overrides = new SearchFieldOverrides();
      if (fields.get("add") instanceof List<?> add) {
        SearchFieldBoostArray boosts = new SearchFieldBoostArray();
        for (Object entry : add) {
          boosts.add(boost((Map<?, ?>) entry));
        }
        overrides.setAdd(boosts);
      }
      if (fields.get("remove") instanceof List<?> remove) {
        StringArray names = new StringArray();
        remove.forEach(name -> names.add(String.valueOf(name)));
        overrides.setRemove(names);
      }
      if (fields.get("override") instanceof List<?> override) {
        SearchFieldBoostArray boosts = new SearchFieldBoostArray();
        for (Object entry : override) {
          boosts.add(boost((Map<?, ?>) entry));
        }
        overrides.setOverride(boosts);
      }
      properties.setFields(overrides);
    }
    return properties;
  }

  @Nonnull
  private static SearchFieldBoost boost(@Nonnull Map<?, ?> entry) {
    SearchFieldBoost boost = new SearchFieldBoost();
    boost.setFieldName(String.valueOf(entry.get("fieldName")));
    boost.setBoost(((Number) entry.get("boost")).floatValue());
    return boost;
  }

  @Nonnull
  private static String unwrap(@Nonnull String query) {
    return query.replace("{{=<% %>=}}", "").replace("<%={{ }}=%>", "").trim();
  }

  @Nonnull
  private static List<Map<String, Object>> readList(@Nonnull String resource) {
    try {
      return YAML.readValue(readUtf8(resource), LIST);
    } catch (Exception e) {
      throw new IllegalStateException("Could not read " + resource, e);
    }
  }

  @Nonnull
  private static String readUtf8(@Nonnull String resource) {
    try (InputStream in = SearchTemplateFixtures.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalStateException("Missing classpath resource " + resource);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Could not read " + resource, e);
    }
  }
}
