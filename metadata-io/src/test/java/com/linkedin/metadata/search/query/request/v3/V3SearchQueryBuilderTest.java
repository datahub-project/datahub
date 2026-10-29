package com.linkedin.metadata.search.query.request.v3;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.DESCRIPTION;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.OTHER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.QUALIFIED_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.STEMMED_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder.STRUCTURED_QUERY_PREFIX;
import static com.linkedin.metadata.search.query.request.SearchQueryBuilderTest.testQueryConfig;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.testng.annotations.Test;

/** The Stage 1 query over the shared {@code _search} fields of the Search V3 indices. */
public class V3SearchQueryBuilderTest {

  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final List<EntitySpec> DATASET =
      List.of(OP_CONTEXT.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
  private static final List<EntitySpec> SCHEMA_FIELD =
      List.of(OP_CONTEXT.getEntityRegistry().getEntitySpec(SCHEMA_FIELD_ENTITY_NAME));

  @Test
  public void testFullQueryIsStage1OverSharedFields() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders table", true);

    assertTrue(
        ((FunctionScoreQueryBuilder) query).query() instanceof DisMaxQueryBuilder,
        query.toString());
    // The fuzzy simple queries, one per analyzer: the shared text and stemmed subfields
    Map<String, Set<String>> fuzzy =
        find(query, SimpleQueryStringBuilder.class).stream()
            .filter(sqs -> sqs.value().contains("~"))
            .collect(
                Collectors.toMap(
                    SimpleQueryStringBuilder::analyzer,
                    sqs -> sqs.fields().keySet(),
                    (a, b) -> {
                      Set<String> merged = new HashSet<>(a);
                      merged.addAll(b);
                      return merged;
                    }));
    Set<String> text = fuzzy.get(TEXT_SEARCH_ANALYZER);
    assertNotNull(text, fuzzy.toString());
    assertTrue(
        text.containsAll(Set.of("name.text", "description.text", "other.text")), text.toString());
    assertFalse(text.contains("name_1.text"), text.toString());
    assertFalse(text.contains("description_1.text"), text.toString());
    assertTrue(text.stream().allMatch(field -> field.endsWith(".text")), text.toString());
    assertTrue(
        fuzzy.values().stream()
            .flatMap(Set::stream)
            .allMatch(field -> field.endsWith(".text") || field.endsWith(".stemmed")),
        fuzzy.toString());
    Set<String> stemmed = fuzzy.get(STEMMED_SEARCH_ANALYZER);
    assertNotNull(stemmed, fuzzy.toString());
    assertTrue(stemmed.contains("name.stemmed"), stemmed.toString());
    assertTrue(stemmed.stream().allMatch(field -> field.endsWith(".stemmed")), stemmed.toString());
  }

  /**
   * Dataset column text and a business attribute's own description share a source field name and
   * land at different degrees. An unqualified query of both reads {@code description} only.
   */
  @Test
  public void testQueryOfDatasetAndBusinessAttributeSkipsColumnDescriptions() {
    EntitySpec businessAttribute =
        OP_CONTEXT.getEntityRegistry().getEntitySpec("businessAttribute");
    QueryBuilder query =
        builder(null)
            .buildQuery(
                OP_CONTEXT, List.of(DATASET.get(0), businessAttribute), "orders table", true);

    Set<String> fields =
        find(query, SimpleQueryStringBuilder.class).stream()
            .flatMap(sqs -> sqs.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertTrue(fields.contains("description.text"), fields.toString());
    assertFalse(fields.contains("description_1.text"), fields.toString());
    assertFalse(fields.contains("description_1.stemmed"), fields.toString());
    assertFalse(fields.contains("tags_1.text"), fields.toString());
    assertFalse(fields.contains("tags_1.stemmed"), fields.toString());
  }

  /** V3 indices have neither the per-field analyzed subfields nor their analyzers. */
  @Test
  public void testNoPerFieldSubfieldsOrAnalyzers() {
    V3SearchQueryBuilder builder = builder(null);
    for (String input :
        List.of(
            "orders",
            "orders table",
            "sales.orders",
            "customer master data for orders",
            "load_job-0001",
            "orders2017",
            "\"orders table\"",
            STRUCTURED_QUERY_PREFIX + "name:orders")) {
      for (boolean light : List.of(false, true)) {
        QueryBuilder query = builder.buildQuery(OP_CONTEXT, DATASET, input, true, light);
        if (query == null) {
          continue;
        }
        String json = query.toString();
        for (String v2Only :
            List.of(".delimited", "wordGrams", "word_gram", ".ngram", "query_word_delimited")) {
          assertFalse(json.contains(v2Only), input + " (light " + light + "): " + json);
        }
      }
    }
  }

  /**
   * Deliberate V3 change: shared-field weights replace @Searchable boost scores in the analyzed
   * clauses, so a name weighs more than a description, and that more than the catch-all.
   */
  @Test
  public void testSharedFieldWeights() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", true);

    Map<String, Float> weights =
        find(query, SimpleQueryStringBuilder.class).stream()
            .filter(sqs -> sqs.value().contains("~") && TEXT_SEARCH_ANALYZER.equals(sqs.analyzer()))
            .findFirst()
            .orElseThrow()
            .fields();
    assertTrue(weights.get("name.text") > weights.get("description.text"), weights.toString());
    assertTrue(weights.get("description.text") > weights.get("other.text"), weights.toString());
    assertFalse(weights.containsKey("_search.structuredProperties.text"), weights.toString());
  }

  /**
   * Exact matches read the root keywords and the urn, with the constant score for an exact name;
   * phrase prefixes and the wildcard read the shared fields that name the entity.
   */
  @Test
  public void testExactPrefixAndWildcardClauses() {
    QueryBuilder query = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", true);
    assertFalse(query.toString().contains("editedDescription"), query.toString());
    QueryBuilder nonFulltext = builder(null).buildQuery(OP_CONTEXT, DATASET, "orders", false);
    assertFalse(nonFulltext.toString().contains("editedDescription"), nonFulltext.toString());
    // A URN is an identity lookup and reuses the all-terms simple query
    QueryBuilder urn =
        builder(null)
            .buildQuery(
                OP_CONTEXT,
                DATASET,
                "urn:li:dataset:(urn:li:dataPlatform:hive,sales.orders,PROD)",
                true);
    Set<String> urnFields =
        find(urn, SimpleQueryStringBuilder.class).stream()
            .flatMap(sqs -> sqs.fields().keySet().stream())
            .collect(Collectors.toSet());
    assertFalse(urnFields.contains("editedDescription"), urnFields.toString());
    assertFalse(urnFields.contains("description"), urnFields.toString());
    assertFalse(urnFields.contains("fieldDescriptions"), urnFields.toString());
    assertTrue(urnFields.contains("description.text"), urnFields.toString());

    assertTrue(
        find(query, ConstantScoreQueryBuilder.class).stream()
            .map(ConstantScoreQueryBuilder::innerQuery)
            .anyMatch(
                inner -> inner instanceof TermQueryBuilder term && term.fieldName().equals("name")),
        query.toString());
    Set<String> exact =
        find(query, TermQueryBuilder.class).stream()
            .map(TermQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertTrue(exact.containsAll(Set.of("name", "urn")), exact.toString());
    assertFalse(exact.contains("name.keyword"), exact.toString());
    assertFalse(exact.contains("urnName"), exact.toString());
    assertTrue(
        exact.stream().noneMatch(field -> field.toLowerCase().contains("description")),
        exact.toString());

    Set<String> prefixes =
        find(query, MatchPhrasePrefixQueryBuilder.class).stream()
            .map(MatchPhrasePrefixQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertTrue(prefixes.contains("name.text"), prefixes.toString());
    // A prefix of the catch-all would match any urn component or custom property
    assertTrue(
        prefixes.stream()
            .allMatch(field -> field.startsWith("name.") || field.startsWith("qualifiedName.")),
        prefixes.toString());
    assertTrue(
        prefixes.stream().noneMatch(field -> field.toLowerCase().contains("description")),
        prefixes.toString());
    assertTrue(
        find(query, WildcardQueryBuilder.class).stream()
            .allMatch(wildcard -> wildcard.fieldName().startsWith("name.")),
        query.toString());
    assertFalse(find(query, WildcardQueryBuilder.class).isEmpty(), query.toString());
  }

  @Test
  public void testFqnAndDescriptionPhraseReadSharedFields() {
    V3SearchQueryBuilder builder = builder(null);

    Set<String> fqn =
        find(builder.buildQuery(OP_CONTEXT, DATASET, "sales.orders", true), MatchQueryBuilder.class)
            .stream()
            .filter(match -> match.operator() == Operator.AND)
            .map(MatchQueryBuilder::fieldName)
            .collect(Collectors.toSet());
    assertEquals(fqn, Set.of("qualifiedName.text", "other.text"));

    assertEquals(
        find(
                builder.buildQuery(OP_CONTEXT, DATASET, "customer master data for orders", true),
                MatchQueryBuilder.class)
            .stream()
            .map(MatchQueryBuilder::fieldName)
            .collect(Collectors.toSet()),
        Set.of("description.text"));
  }

  @Test
  public void testStructuredPropertyFieldsUseDefinitionWeights() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder queryBuilder = builder(null);
    QueryBuilder query = queryBuilder.buildQuery(context, dataset, "orders table", true);
    assertFalse(query.toString().contains("_search.structuredProperties"), query.toString());
    assertFalse(query.toString().contains("customFullTextSearchFields"), query.toString());

    Map<String, Float> text = analyzerFields(query, V3SearchFields.TEXT_SEARCH_ANALYZER);
    Map<String, Float> stemmed = analyzerFields(query, V3SearchFields.STEMMED_SEARCH_ANALYZER);
    float description = text.get("description.text");
    assertEquals(text.get("structuredProperties.retentionPolicy.text") / description, 0.8f, 0.001f);
    assertEquals(
        stemmed.get("structuredProperties.retentionPolicy.stemmed") / description, 0.4f, 0.001f);
    assertEquals(text.get("structuredProperties.weightedNote.text") / description, 3.0f, 0.001f);
    assertEquals(
        stemmed.get("structuredProperties.weightedNote.stemmed") / description, 1.25f, 0.001f);
    assertFalse(text.containsKey("structuredProperties.hiddenLabel.text"), text.toString());

    SimpleQueryStringBuilder propertyClause =
        find(query, SimpleQueryStringBuilder.class).stream()
            .filter(
                clause -> clause.fields().containsKey("structuredProperties.retentionPolicy.text"))
            .findFirst()
            .orElseThrow();
    assertTrue(propertyClause.lenient(), propertyClause.toString());
    assertFalse(propertyClause.fields().containsKey("name.text"), propertyClause.toString());

    Set<String> singleWord =
        only(find(
                queryBuilder.buildQuery(context, dataset, "orders", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertTrue(
        singleWord.stream().noneMatch(field -> field.startsWith("structuredProperties.")),
        singleWord.toString());
    assertFalse(singleWord.contains("customFullTextSearchFields"), singleWord.toString());

    Set<String> severalWords =
        only(find(
                queryBuilder.buildQuery(context, dataset, "orders table", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertTrue(
        severalWords.containsAll(
            Set.of(
                "structuredProperties.retentionPolicy.text",
                "structuredProperties.weightedNote.stemmed")),
        severalWords.toString());
    assertFalse(
        severalWords.contains("structuredProperties.hiddenLabel.text"), severalWords.toString());
    assertTrue(
        severalWords.stream().anyMatch(field -> !field.startsWith("_search.")),
        severalWords.toString());
  }

  @Test
  public void testCatalogMissKeepsPropertyFieldsAfterNestedQuery() {
    V3SearchQueryBuilder queryBuilder = builder(null);
    OperationContext[] context = new OperationContext[1];
    context[0] =
        propertyContext(
            new SearchRetriever() {
              @Override
              public ScrollResult scroll(
                  @Nonnull List<String> entities,
                  @Nullable Filter filters,
                  @Nullable String scrollId,
                  @Nullable Integer count,
                  List<SortCriterion> sortCriteria,
                  @Nullable SearchFlags searchFlags) {
                // The production retriever builds this listing through the same query builder.
                queryBuilder.buildQuery(
                    context[0],
                    List.of(
                        context[0]
                            .getEntityRegistry()
                            .getEntitySpec(STRUCTURED_PROPERTY_ENTITY_NAME)),
                    "*",
                    false);
                return structuredPropertyPage(
                    scrollId,
                    List.of(
                        UrnUtils.getUrn("urn:li:structuredProperty:retentionPolicy"),
                        UrnUtils.getUrn("urn:li:structuredProperty:weightedNote"),
                        UrnUtils.getUrn("urn:li:structuredProperty:hiddenLabel")));
              }
            });
    List<EntitySpec> dataset =
        List.of(context[0].getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));

    Map<String, Float> text =
        analyzerFields(
            queryBuilder.buildQuery(context[0], dataset, "orders table", true),
            V3SearchFields.TEXT_SEARCH_ANALYZER);

    assertTrue(text.containsKey("structuredProperties.retentionPolicy.text"), text.toString());
    assertTrue(text.containsKey("structuredProperties.weightedNote.text"), text.toString());
    assertFalse(text.containsKey("structuredProperties.hiddenLabel.text"), text.toString());
  }

  @Test
  public void testFieldConfigurationSelectsStructuredPropertiesByPath() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "addHidden",
                        searchFields(
                            SearchFields.builder()
                                .add(List.of("structuredProperties.hiddenLabel"))
                                .build()),
                        "dropProperties",
                        searchFields(
                            SearchFields.builder().remove(List.of("structuredProperties")).build()),
                        "onlyRetention",
                        searchFields(
                            SearchFields.builder()
                                .replace(List.of("structuredProperties.retentionPolicy"))
                                .build())))
                .build());

    Set<String> added =
        analyzerFields(
                configured.buildQuery(
                    context.withSearchFlags(flags -> flags.setFieldConfiguration("addHidden")),
                    dataset,
                    "orders table",
                    true),
                V3SearchFields.TEXT_SEARCH_ANALYZER)
            .keySet();
    assertTrue(added.contains("structuredProperties.hiddenLabel.text"), added.toString());

    Set<String> dropped =
        analyzerFields(
                configured.buildQuery(
                    context.withSearchFlags(flags -> flags.setFieldConfiguration("dropProperties")),
                    dataset,
                    "orders table",
                    true),
                V3SearchFields.TEXT_SEARCH_ANALYZER)
            .keySet();
    assertTrue(
        dropped.stream().noneMatch(field -> field.startsWith("structuredProperties.")),
        dropped.toString());

    Set<String> replaced =
        analyzerFields(
                configured.buildQuery(
                    context.withSearchFlags(flags -> flags.setFieldConfiguration("onlyRetention")),
                    dataset,
                    "orders table",
                    true),
                V3SearchFields.TEXT_SEARCH_ANALYZER)
            .keySet();
    assertTrue(replaced.contains("structuredProperties.retentionPolicy.text"), replaced.toString());
    assertFalse(replaced.contains("name.text"), replaced.toString());
    assertFalse(replaced.contains("structuredProperties.weightedNote.text"), replaced.toString());
  }

  @Test
  public void testLightQueryReadsSharedFields() {
    V3SearchQueryBuilder builder = builder(null);

    // A single name searches only the shared fields that name the entity
    Set<String> name =
        only(find(
                builder.buildQuery(OP_CONTEXT, DATASET, "orders", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertEquals(
        name, Set.of("name.text", "name.stemmed", "qualifiedName.text", "qualifiedName.stemmed"));
    assertFalse(name.contains("customFullTextSearchFields"), name.toString());

    // Several words search every shared field
    Set<String> words =
        only(find(
                builder.buildQuery(OP_CONTEXT, DATASET, "orders table", true, true),
                MultiMatchQueryBuilder.class))
            .fields()
            .keySet();
    assertTrue(words.contains("description.text"), words.toString());
    assertFalse(words.contains("name_1.stemmed"), words.toString());
    assertFalse(words.contains("description_1.text"), words.toString());
    assertTrue(
        words.stream()
            .allMatch(
                field ->
                    field.matches("(_search\\.)?[A-Za-z]+(_\\d+)?(\\.[A-Za-z]+)?\\.(text|stemmed)")
                        || field.contains("__urn_name.")),
        words.toString());
    assertTrue(
        words.stream().noneMatch(field -> field.startsWith("structuredProperties.")),
        words.toString());

    // An identifier the escaping breaks up is matched whole on the names
    List<MultiMatchQueryBuilder> identity =
        find(
                builder.buildQuery(OP_CONTEXT, DATASET, "load_job-0001", true, true),
                MultiMatchQueryBuilder.class)
            .stream()
            .filter(match -> match.value().equals("load_job-0001"))
            .collect(Collectors.toList());
    assertEquals(only(identity).fields().keySet(), Set.of("name.text", "qualifiedName.text"));
  }

  @Test
  public void testStructuredQuerySplitsPropertiesAndSkipsHiddenOnes() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    QueryBuilder query = builder(null).buildQuery(context, dataset, "orders", false);

    List<QueryStringQueryBuilder> clauses = find(query, QueryStringQueryBuilder.class);
    // Unset lenient is null, which is strict
    List<QueryStringQueryBuilder> strict =
        clauses.stream().filter(clause -> !Boolean.TRUE.equals(clause.lenient())).toList();
    List<QueryStringQueryBuilder> lenient =
        clauses.stream().filter(clause -> Boolean.TRUE.equals(clause.lenient())).toList();
    assertFalse(strict.isEmpty(), clauses.toString());
    assertEquals(lenient.size(), 1, clauses.toString());
    assertTrue(
        strict.stream().anyMatch(clause -> clause.fields().containsKey("name.text")),
        strict.toString());
    assertTrue(
        strict.stream()
            .flatMap(clause -> clause.fields().keySet().stream())
            .noneMatch(StructuredPropertyUtils::isStructuredPropertyField),
        strict.toString());
    assertEquals(
        lenient.get(0).fields().keySet().stream()
            .filter(StructuredPropertyUtils::isStructuredPropertyField)
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties.retentionPolicy.text",
            "structuredProperties.retentionPolicy.stemmed",
            "structuredProperties.weightedNote.text",
            "structuredProperties.weightedNote.stemmed"));
  }

  @Test
  public void testUrnStructuredPropertyExactBoostReadsLowercaseName() {
    Urn steward = UrnUtils.getUrn("urn:li:structuredProperty:steward");
    UrnArray datasets = new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset"));
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setQualifiedName("steward")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"))
            .setEntityTypes(datasets)
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    MockAspectRetriever definitions = new MockAspectRetriever(Map.of(steward, List.of(definition)));
    definitions.setEntityRegistry(TestOperationContexts.defaultEntityRegistry());
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(definitions)
            .cachingAspectRetriever(definitionsWithActiveUsers(definitions))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(structuredPropertySearchRetriever(List.of(steward)))
            .build();
    OperationContext context =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverContext);
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    String query = builder(null).buildQuery(context, dataset, "Confidential", false).toString();
    assertTrue(query.contains("structuredProperties.steward__urn_name.lowercase"), query);
    assertTrue(query.contains("structuredProperties.steward__urn_name.text"), query);

    String folded = builder(null).buildQuery(context, dataset, "JOS\u00c9", false).toString();
    String lowercase = clause(folded, "\"structuredProperties.steward__urn_name.lowercase\" :");
    assertTrue(lowercase.contains("\"value\" : \"jose\""), lowercase);
    String original = clause(folded, "\"structuredProperties.steward__urn_name\" :");
    assertTrue(original.contains("\"value\" : \"JOS\u00c9\""), original);
  }

  @Test
  public void testExactMatchFoldsNormalizedKeywords() {
    String query = builder(null).buildQuery(OP_CONTEXT, DATASET, "JOS\u00c9", false).toString();
    String tags = clause(query, "\"tags\" :");
    assertTrue(tags.contains("\"value\" : \"jose\""), tags);
    assertFalse(tags.contains("JOS"), tags);

    String name = clause(query, "\"name\" :");
    assertTrue(name.contains("\"value\" : \"JOS\u00c9\""), name);
  }

  private static String clause(String query, String field) {
    int at = query.indexOf(field);
    assertTrue(at > 0, query);
    return query.substring(at, Math.min(query.length(), at + 160));
  }

  @Test
  public void testStructuredQueryDefaultFields() {
    QueryBuilder query =
        builder(null)
            .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:orders", true);

    // name maps .lowercase, so an unquoted value is a normalized term on that subfield.
    keywordTerm(query, "name.lowercase", "orders");
    assertTrue(find(query, QueryStringQueryBuilder.class).isEmpty());
    assertTrue(find(query, SimpleQueryStringBuilder.class).isEmpty());
  }

  /** A single wildcard on name reads the lowercase subfield. */
  @Test
  public void testStructuredQueryWildcardReadsLowercaseSubfield() {
    QueryBuilder mixed =
        builder(null)
            .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:Orders*", true);
    List<WildcardQueryBuilder> wildcards = find(mixed, WildcardQueryBuilder.class);
    assertEquals(wildcards.size(), 1, wildcards.toString());
    assertEquals(wildcards.get(0).fieldName(), "name.lowercase");
    assertEquals(wildcards.get(0).value(), "orders*");
    assertFalse(wildcards.get(0).caseInsensitive());
    assertTrue(find(mixed, QueryStringQueryBuilder.class).isEmpty());

    List<WildcardQueryBuilder> lower =
        find(
            builder(null)
                .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:orders*", true),
            WildcardQueryBuilder.class);
    assertEquals(lower.size(), 1, lower.toString());
    assertEquals(lower.get(0).fieldName(), "name.lowercase");
    assertEquals(lower.get(0).value(), "orders*");
    assertFalse(lower.get(0).caseInsensitive());

    QueryStringQueryBuilder parsed =
        only(
            find(
                builder(null)
                    .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "tags:Pii*", true),
                QueryStringQueryBuilder.class));
    assertEquals(parsed.queryString(), "tags:Pii*");

    Set<String> description =
        find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "description:Orders*", true),
                QueryStringQueryBuilder.class)
            .stream()
            .map(QueryStringQueryBuilder::queryString)
            .collect(Collectors.toSet());
    assertTrue(description.contains("description.text:Orders*"), description.toString());
    assertTrue(description.contains("description.stemmed:Orders*"), description.toString());

    QueryStringQueryBuilder grouped =
        only(
            find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:(Orders*)", true),
                QueryStringQueryBuilder.class));
    assertEquals(grouped.queryString(), "name:(Orders*)");

    QueryStringQueryBuilder boosted =
        only(
            find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:Orders*^2", true),
                QueryStringQueryBuilder.class));
    assertEquals(boosted.queryString(), "name:Orders*^2");
    assertTrue(
        find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:Orders*^2", true),
                WildcardQueryBuilder.class)
            .isEmpty());

    QueryStringQueryBuilder fuzzy =
        only(
            find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:Orders*~1", true),
                QueryStringQueryBuilder.class));
    assertEquals(fuzzy.queryString(), "name:Orders*~1");

    QueryStringQueryBuilder termBoost =
        only(
            find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "name:orders^2", true),
                QueryStringQueryBuilder.class));
    assertEquals(termBoost.queryString(), "name:orders^2");
  }

  /**
   * A fielded /q runs that field once. An unquoted value on a keyword root is one case-insensitive
   * term. A property field stays one lenient query_string, so an unmapped property cannot fail a
   * strict clause.
   */
  @Test
  public void testFieldedStructuredQueryDoesNotScoreTheNamedFieldTwice() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder queryBuilder = builder(null);

    QueryBuilder mixedQuery =
        queryBuilder.buildQuery(context, dataset, "orders AND name:foo", false);
    List<QueryStringQueryBuilder> mixed = queryStrings(mixedQuery);
    assertEquals(mixed.size(), 2, mixed.toString());
    assertEquals(mixed.stream().filter(clause -> clause.queryString().equals("orders")).count(), 2);
    assertTrue(
        mixed.stream().noneMatch(clause -> clause.queryString().startsWith("name:")),
        mixed.toString());
    keywordTerm(mixedQuery, "name.lowercase", "foo");

    QueryBuilder namedQuery = queryBuilder.buildQuery(context, dataset, "name:orders", false);
    assertTrue(queryStrings(namedQuery).isEmpty(), queryStrings(namedQuery).toString());
    keywordTerm(namedQuery, "name.lowercase", "orders");

    List<QueryStringQueryBuilder> property =
        queryStrings(
            queryBuilder.buildQuery(
                context, dataset, "structuredProperties.retentionPolicy:gold", false));
    assertEquals(property.size(), 1, property.toString());
    assertEquals(property.get(0).queryString(), "structuredProperties.retentionPolicy:gold");
    assertEquals(property.get(0).lenient(), Boolean.TRUE);

    // Not queried by default, and still only on the lenient clause.
    List<QueryStringQueryBuilder> hidden =
        queryStrings(
            queryBuilder.buildQuery(
                context, dataset, "structuredProperties.hiddenLabel:gold", false));
    assertEquals(hidden.size(), 1, hidden.toString());
    assertEquals(hidden.get(0).queryString(), "structuredProperties.hiddenLabel:gold");
    assertEquals(hidden.get(0).lenient(), Boolean.TRUE);

    QueryBuilder conjunctionQuery =
        queryBuilder.buildQuery(
            context, dataset, "name:orders AND structuredProperties.retentionPolicy:gold", false);
    List<QueryStringQueryBuilder> both = queryStrings(conjunctionQuery);
    assertEquals(both.size(), 1, both.toString());
    assertEquals(both.get(0).queryString(), "structuredProperties.retentionPolicy:gold");
    assertEquals(both.get(0).lenient(), Boolean.TRUE);
    keywordTerm(conjunctionQuery, "name.lowercase", "orders");
    BoolQueryBuilder conjunction =
        find(conjunctionQuery, BoolQueryBuilder.class).stream()
            .filter(clause -> clause.must().size() == 2)
            .findFirst()
            .orElseThrow();
    assertEquals(
        conjunction.must().stream().map(QueryBuilder::getClass).collect(Collectors.toSet()),
        Set.of(TermQueryBuilder.class, QueryStringQueryBuilder.class));

    QueryBuilder eitherQuery =
        queryBuilder.buildQuery(
            context, dataset, "name:orders OR structuredProperties.hiddenLabel:gold", false);
    List<QueryStringQueryBuilder> either = queryStrings(eitherQuery);
    assertEquals(either.size(), 1, either.toString());
    assertEquals(either.get(0).queryString(), "structuredProperties.hiddenLabel:gold");
    assertEquals(either.get(0).lenient(), Boolean.TRUE);
    keywordTerm(eitherQuery, "name.lowercase", "orders");

    // The field applies to the whole group, so the group stays one clause and is lenient.
    List<QueryStringQueryBuilder> group =
        queryStrings(
            queryBuilder.buildQuery(
                context,
                dataset,
                "name:(orders OR structuredProperties.retentionPolicy:gold)",
                false));
    assertEquals(group.size(), 1, group.toString());
    assertEquals(
        group.get(0).queryString(), "name:(orders OR structuredProperties.retentionPolicy:gold)");
    assertEquals(group.get(0).lenient(), Boolean.TRUE);
  }

  /** An unquoted /q value on a text-only label searches its analyzed subfields, not the root. */
  @Test
  public void testStructuredQueryRewritesUnquotedTextOnlyLabels() {
    QueryBuilder query =
        builder(null)
            .buildQuery(
                OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "description:warehouse", true);

    Set<String> texts =
        find(query, QueryStringQueryBuilder.class).stream()
            .map(QueryStringQueryBuilder::queryString)
            .collect(Collectors.toSet());
    assertTrue(texts.contains("description.text:warehouse"), texts.toString());
    assertTrue(texts.contains("description.stemmed:warehouse"), texts.toString());
    assertFalse(texts.contains("description:warehouse"), texts.toString());
  }

  /** A quoted /q value stays on exact intent and is not promoted to the analyzed subfields. */
  @Test
  public void testStructuredQueryQuotedTextOnlyLabelStaysExact() {
    QueryBuilder query =
        builder(null)
            .buildQuery(
                OP_CONTEXT,
                DATASET,
                STRUCTURED_QUERY_PREFIX + "description:\"data warehouse\"",
                true);

    Set<String> texts =
        find(query, QueryStringQueryBuilder.class).stream()
            .map(QueryStringQueryBuilder::queryString)
            .collect(Collectors.toSet());
    assertTrue(texts.contains("description:\"data warehouse\""), texts.toString());
    assertTrue(
        texts.stream().noneMatch(text -> text.contains(".text") || text.contains(".stemmed")),
        texts.toString());
  }

  /** A keyword label is not a text-only field, so /q leaves the field name as written. */
  @Test
  public void testStructuredQueryKeywordLabelStaysOnKeywordField() {
    QueryStringQueryBuilder structured =
        only(
            find(
                builder(null)
                    .buildQuery(OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "tags:pii", true),
                QueryStringQueryBuilder.class));
    assertEquals(structured.queryString(), "tags:pii");
  }

  /** A source name of a text-only label uses the label's analyzed fields, the same as CONTAIN. */
  @Test
  public void testStructuredQuerySourceNameUsesContainAnalyzedFields() {
    QueryBuilder query =
        builder(null)
            .buildQuery(
                OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "fieldDescriptions:warehouse", true);

    Set<String> texts =
        find(query, QueryStringQueryBuilder.class).stream()
            .map(QueryStringQueryBuilder::queryString)
            .collect(Collectors.toSet());
    assertTrue(texts.contains("description_1.text:warehouse"), texts.toString());
    assertTrue(texts.contains("description_1.stemmed:warehouse"), texts.toString());
    assertFalse(texts.contains("fieldDescriptions:warehouse"), texts.toString());
  }

  /** A source name of an indexed label uses the keyword path CONTAIN would wildcard. */
  @Test
  public void testStructuredQuerySourceNameUsesContainKeywordPath() {
    QueryStringQueryBuilder structured =
        only(
            find(
                builder(null)
                    .buildQuery(
                        OP_CONTEXT, DATASET, STRUCTURED_QUERY_PREFIX + "fieldPaths:user_id", true),
                QueryStringQueryBuilder.class));
    assertEquals(structured.queryString(), "name_1:user_id");
  }

  /** A space after the field colon is valid query-string syntax and must keep the split. */
  @Test
  public void testFieldedStructuredQueryAllowsSpaceAfterColon() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder queryBuilder = builder(null);

    QueryBuilder namedQuery = queryBuilder.buildQuery(context, dataset, "name: orders", false);
    assertTrue(queryStrings(namedQuery).isEmpty(), queryStrings(namedQuery).toString());
    keywordTerm(namedQuery, "name.lowercase", "orders");

    List<QueryStringQueryBuilder> quoted =
        queryStrings(queryBuilder.buildQuery(context, dataset, "name: \"foo\"", false));
    assertEquals(quoted.size(), 1, quoted.toString());
    assertEquals(quoted.get(0).queryString(), "name: \"foo\"");
    assertNotEquals(Boolean.TRUE, quoted.get(0).lenient());

    List<QueryStringQueryBuilder> property =
        queryStrings(
            queryBuilder.buildQuery(
                context, dataset, "structuredProperties.retentionPolicy: gold", false));
    assertEquals(property.size(), 1, property.toString());
    assertEquals(property.get(0).queryString(), "structuredProperties.retentionPolicy: gold");
    assertEquals(property.get(0).lenient(), Boolean.TRUE);

    QueryBuilder bothQuery =
        queryBuilder.buildQuery(
            context, dataset, "name: orders AND structuredProperties.retentionPolicy: gold", false);
    List<QueryStringQueryBuilder> both = queryStrings(bothQuery);
    assertEquals(both.size(), 1, both.toString());
    assertEquals(both.get(0).queryString(), "structuredProperties.retentionPolicy: gold");
    assertEquals(both.get(0).lenient(), Boolean.TRUE);
    keywordTerm(bothQuery, "name.lowercase", "orders");
  }

  @Test
  public void testUnqualifiedStructuredQueryStaysOneSplit() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder queryBuilder = builder(null);

    for (String input :
        List.of(
            "orders",
            "orders AND table",
            "\"name:orders\"",
            "\"name:orders\" AND table",
            "foo\\:bar",
            "/a:b/ AND orders")) {
      List<QueryStringQueryBuilder> clauses =
          queryStrings(queryBuilder.buildQuery(context, dataset, input, false));
      assertEquals(clauses.size(), 2, input + " " + clauses);
      assertTrue(
          clauses.stream().allMatch(clause -> clause.queryString().equals(input)),
          clauses.toString());
      assertEquals(
          clauses.stream().filter(clause -> Boolean.TRUE.equals(clause.lenient())).count(),
          1,
          clauses.toString());
    }
  }

  @Test
  public void testFieldedPropertyStaysLenientWhenPropertiesAreRemoved() {
    OperationContext context = propertyContext();
    List<EntitySpec> dataset =
        List.of(context.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME));
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "dropProperties",
                        searchFields(
                            SearchFields.builder()
                                .remove(List.of("structuredProperties"))
                                .build())))
                .build());

    List<QueryStringQueryBuilder> clauses =
        queryStrings(
            configured.buildQuery(
                context.withSearchFlags(flags -> flags.setFieldConfiguration("dropProperties")),
                dataset,
                "structuredProperties.retentionPolicy:gold",
                false));
    assertEquals(clauses.size(), 1, clauses.toString());
    assertEquals(clauses.get(0).queryString(), "structuredProperties.retentionPolicy:gold");
    assertEquals(clauses.get(0).lenient(), Boolean.TRUE);
  }

  /**
   * Deliberate V3 change: a field configuration names shared fields, directly or by a field that
   * feeds one, so removing fieldPaths removes the whole name_1 field.
   */
  @Test
  public void testFieldConfigurationsSelectSharedFields() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "names",
                        searchFields(SearchFields.builder().replace(List.of("name")).build()),
                        "noColumns",
                        searchFields(SearchFields.builder().remove(List.of("fieldPaths")).build()),
                        "invalid",
                        searchFields(
                            SearchFields.builder()
                                .replace(List.of("name"))
                                .add(List.of("description"))
                                .build())))
                .build());

    assertEquals(
        configured.searchedFields(withFieldConfiguration("names"), DATASET).keySet(),
        Set.of(ENTITY_NAME));
    // Only the entity name, and the root fields that feed it
    List<String> nameRoots = configured.searchedFields(OP_CONTEXT, DATASET).get(ENTITY_NAME);
    for (boolean light : List.of(false, true)) {
      QueryBuilder names =
          configured.buildQuery(withFieldConfiguration("names"), DATASET, "orders", true, light);
      Set<String> fields = new HashSet<>();
      find(names, SimpleQueryStringBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
      find(names, MultiMatchQueryBuilder.class).forEach(q -> fields.addAll(q.fields().keySet()));
      assertTrue(fields.contains("name.text"), fields.toString());
      assertTrue(
          fields.stream().allMatch(field -> field.startsWith("name.") || nameRoots.contains(field)),
          fields.toString());
    }

    Set<String> noColumns =
        configured.searchedFields(withFieldConfiguration("noColumns"), DATASET).keySet();
    assertFalse(noColumns.contains("name_1"), noColumns.toString());
    assertTrue(noColumns.contains(DESCRIPTION), noColumns.toString());
    for (boolean light : List.of(false, true)) {
      String query =
          configured
              .buildQuery(withFieldConfiguration("noColumns"), DATASET, "orders table", true, light)
              .toString();
      assertTrue(query.contains("description.text"), query);
      assertFalse(query.contains("name_1"), query);
      assertFalse(query.contains("columns"), query);
      assertFalse(query.contains("\"fieldPaths"), query);
    }
    // Replace cannot be combined with add: the configuration is ignored
    assertEquals(
        configured.searchedFields(withFieldConfiguration("invalid"), DATASET),
        configured.searchedFields(OP_CONTEXT, DATASET));
  }

  /**
   * A generated analyzer path is selected by its shared field. A dotted source path is selected by
   * the shared field its root feeds, which is {@code other} when the reference names no label. The
   * keyword parent that exact match rewrites to {@code .keyword} stays with that source. A keyword
   * root is left out of the fuzzy simple query; a structured query still lists it.
   */
  @Test
  public void testFieldConfigurationKeepsDottedSourcesThatFeedTheSelectedField() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "names",
                        searchFields(SearchFields.builder().replace(List.of("name")).build()),
                        "other",
                        searchFields(SearchFields.builder().replace(List.of("other")).build()),
                        "tags",
                        searchFields(SearchFields.builder().replace(List.of("tags")).build())))
                .build());

    Set<String> names = simpleFields(configured, withFieldConfiguration("names"), DATASET);
    assertTrue(names.contains("name.text"), names.toString());
    assertFalse(names.contains("name"), names.toString());
    assertFalse(names.contains("description.text"), names.toString());
    assertFalse(names.contains("other.text"), names.toString());
    assertFalse(names.contains("customProperties"), names.toString());
    assertTrue(termFields(configured, withFieldConfiguration("names"), DATASET).contains("name"));

    Set<String> other = simpleFields(configured, withFieldConfiguration("other"), DATASET);
    assertTrue(other.contains("other.text"), other.toString());
    assertFalse(other.contains("customProperties"), other.toString());
    assertTrue(
        structuredFields(configured, withFieldConfiguration("other"), DATASET)
            .contains("customProperties"));
    assertFalse(other.contains("name.text"), other.toString());
    assertFalse(other.contains("description.text"), other.toString());

    Set<String> schemaOther =
        simpleFields(configured, withFieldConfiguration("other"), SCHEMA_FIELD);
    assertFalse(schemaOther.contains("businessAttributeRef.name"), schemaOther.toString());
    assertTrue(
        structuredFields(configured, withFieldConfiguration("other"), SCHEMA_FIELD)
            .contains("businessAttributeRef.name"));
    assertTrue(schemaOther.contains("other.text"), schemaOther.toString());
    assertFalse(schemaOther.contains("tags.text"), schemaOther.toString());

    Set<String> schemaTags = simpleFields(configured, withFieldConfiguration("tags"), SCHEMA_FIELD);
    assertTrue(schemaTags.contains("tags.text"), schemaTags.toString());
    assertFalse(schemaTags.contains("tags.urnName.text"), schemaTags.toString());
    assertFalse(schemaTags.contains("other.text"), schemaTags.toString());
  }

  /** A long-tail field does not stand for the whole catch-all other field. */
  @Test
  public void testFieldConfigurationNamingALongTailFieldKeepsOther() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "noTags",
                        searchFields(SearchFields.builder().remove(List.of("tags")).build())))
                .build());

    assertTrue(
        configured.searchedFields(withFieldConfiguration("noTags"), DATASET).containsKey(OTHER));
  }

  /** A field configuration that removes every shared field searches them all, as V2 does. */
  @Test
  public void testFieldConfigurationRemovingEveryFieldKeepsTheBaseFields() {
    V3SearchQueryBuilder configured =
        builder(
            CustomSearchConfiguration.builder()
                .fieldConfigurations(
                    Map.of(
                        "nothing",
                        searchFields(
                            SearchFields.builder()
                                .remove(
                                    List.of(
                                        ENTITY_NAME,
                                        "name_1",
                                        QUALIFIED_NAME,
                                        DESCRIPTION,
                                        "description_1",
                                        "tags",
                                        "tags_1",
                                        "glossaryTerms",
                                        "glossaryTerms_1",
                                        "structuredProperties",
                                        OTHER))
                                .build())))
                .build());

    Map<String, List<String>> remaining =
        configured.searchedFields(withFieldConfiguration("nothing"), DATASET);
    // Removing every remaining shared label leaves nothing, so the base fields are restored.
    // Reference fields such as platform are not in that base set.
    assertTrue(remaining.containsKey(ENTITY_NAME), remaining.toString());
    assertTrue(remaining.containsKey(QUALIFIED_NAME), remaining.toString());
    assertTrue(remaining.containsKey(DESCRIPTION), remaining.toString());
    assertTrue(remaining.containsKey("tags"), remaining.toString());
    assertTrue(remaining.containsKey("glossaryTerms"), remaining.toString());
    assertTrue(remaining.containsKey(OTHER), remaining.toString());
    assertFalse(remaining.containsKey("platform"), remaining.toString());
    assertFalse(remaining.containsKey("urnName"), remaining.toString());
    assertFalse(
        remaining.keySet().stream().anyMatch(key -> key.contains("__urn_name")),
        remaining.toString());
  }

  /**
   * As on V2, the production query configuration skips the simple queries, one per analyzer, for a
   * quoted query; the all-terms match of the names still applies.
   */
  @Test
  public void testProductionConfigurationSkipsSimpleQueryForQuotedQuery() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config.yaml");
    V3SearchQueryBuilder production = builder(customConfiguration.resolve(new YAMLMapper()));

    QueryBuilder quoted = production.buildQuery(OP_CONTEXT, DATASET, "\"orders table\"", true);
    assertTrue(
        find(quoted, SimpleQueryStringBuilder.class).stream()
            .allMatch(simple -> simple.analyzer() == null),
        quoted.toString());
    assertFalse(find(quoted, MatchPhrasePrefixQueryBuilder.class).isEmpty(), quoted.toString());

    QueryBuilder plain = production.buildQuery(OP_CONTEXT, DATASET, "orders table", true);
    assertTrue(
        find(plain, SimpleQueryStringBuilder.class).stream()
            .anyMatch(simple -> TEXT_SEARCH_ANALYZER.equals(simple.analyzer())),
        plain.toString());
  }

  private static Map<String, Float> analyzerFields(QueryBuilder query, String analyzer) {
    Map<String, Float> fields = new HashMap<>();
    find(query, SimpleQueryStringBuilder.class).stream()
        .filter(clause -> clause.value().contains("~") && analyzer.equals(clause.analyzer()))
        .forEach(clause -> fields.putAll(clause.fields()));
    return fields;
  }

  private static OperationContext propertyContext() {
    return propertyContext(
        structuredPropertySearchRetriever(
            List.of(
                UrnUtils.getUrn("urn:li:structuredProperty:retentionPolicy"),
                UrnUtils.getUrn("urn:li:structuredProperty:weightedNote"),
                UrnUtils.getUrn("urn:li:structuredProperty:hiddenLabel"))));
  }

  private static OperationContext propertyContext(SearchRetriever searchRetriever) {
    MockAspectRetriever definitions = propertyDefinitions();
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(definitions)
            .cachingAspectRetriever(definitionsWithActiveUsers(definitions))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(searchRetriever)
            .build();
    return TestOperationContexts.systemContextNoSearchAuthorization(retrieverContext);
  }

  private static MockAspectRetriever propertyDefinitions() {
    Urn retention = UrnUtils.getUrn("urn:li:structuredProperty:retentionPolicy");
    Urn weighted = UrnUtils.getUrn("urn:li:structuredProperty:weightedNote");
    Urn hidden = UrnUtils.getUrn("urn:li:structuredProperty:hiddenLabel");
    UrnArray datasets = new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset"));
    StructuredPropertyDefinition retentionPolicy =
        new StructuredPropertyDefinition()
            .setQualifiedName("retentionPolicy")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(datasets)
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    StructuredPropertyDefinition weightedNote =
        new StructuredPropertyDefinition()
            .setQualifiedName("weightedNote")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(datasets))
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setBoostScore(3.0)
                    .setSubfieldBoostScores(new DoubleMap(Map.of("stemmed", 1.25d))));
    StructuredPropertyDefinition hiddenLabel =
        new StructuredPropertyDefinition()
            .setQualifiedName("hiddenLabel")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(datasets))
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(false));
    MockAspectRetriever retriever =
        new MockAspectRetriever(
            Map.of(
                retention, List.of(retentionPolicy),
                weighted, List.of(weightedNote),
                hidden, List.of(hiddenLabel)));
    retriever.setEntityRegistry(TestOperationContexts.defaultEntityRegistry());
    return retriever;
  }

  private static CachingAspectRetriever definitionsWithActiveUsers(
      MockAspectRetriever definitions) {
    CachingAspectRetriever activeUsers =
        TestOperationContexts.emptyActiveUsersAspectRetriever(definitions::getEntityRegistry);
    return new CachingAspectRetriever() {
      @Nonnull
      @Override
      public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
          @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
        Map<Urn, Map<String, Aspect>> found =
            new HashMap<>(definitions.getLatestAspectObjects(context, urns, aspectNames));
        Set<Urn> missing = new HashSet<>();
        for (Urn urn : urns) {
          if (!found.containsKey(urn)) {
            missing.add(urn);
          }
        }
        if (!missing.isEmpty()) {
          found.putAll(activeUsers.getLatestAspectObjects(context, missing, aspectNames));
        }
        return found;
      }

      @Nonnull
      @Override
      public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
          @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
        return definitions.getLatestSystemAspects(context, urnAspectNames);
      }

      @Nonnull
      @Override
      public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
        return definitions.entityExists(context, urns);
      }

      @Nonnull
      @Override
      public EntityRegistry getEntityRegistry() {
        return definitions.getEntityRegistry();
      }
    };
  }

  private static SearchRetriever structuredPropertySearchRetriever(List<Urn> propertyUrns) {
    return new SearchRetriever() {
      @Override
      public ScrollResult scroll(
          @Nonnull List<String> entities,
          @Nullable Filter filters,
          @Nullable String scrollId,
          @Nullable Integer count,
          List<SortCriterion> sortCriteria,
          @Nullable SearchFlags searchFlags) {
        return structuredPropertyPage(scrollId, propertyUrns);
      }
    };
  }

  private static ScrollResult structuredPropertyPage(
      @Nullable String scrollId, List<Urn> propertyUrns) {
    SearchEntityArray results = new SearchEntityArray();
    if (scrollId == null) {
      propertyUrns.forEach(urn -> results.add(new SearchEntity().setEntity(urn)));
    }
    return new ScrollResult()
        .setEntities(results)
        .setNumEntities(results.size())
        .setPageSize(results.size());
  }

  private static V3SearchQueryBuilder builder(CustomSearchConfiguration customConfiguration) {
    return new V3SearchQueryBuilder(testQueryConfig, customConfiguration);
  }

  private static FieldConfiguration searchFields(SearchFields searchFields) {
    return FieldConfiguration.builder().searchFields(searchFields).build();
  }

  private static Set<String> simpleFields(
      V3SearchQueryBuilder builder, OperationContext context, List<EntitySpec> entities) {
    return find(
            builder.buildQuery(context, entities, "orders", true), SimpleQueryStringBuilder.class)
        .stream()
        .flatMap(query -> query.fields().keySet().stream())
        .collect(Collectors.toSet());
  }

  private static Set<String> structuredFields(
      V3SearchQueryBuilder builder, OperationContext context, List<EntitySpec> entities) {
    return find(
            builder.buildQuery(context, entities, STRUCTURED_QUERY_PREFIX + "orders", true),
            QueryStringQueryBuilder.class)
        .stream()
        .flatMap(query -> query.fields().keySet().stream())
        .collect(Collectors.toSet());
  }

  private static Set<String> termFields(
      V3SearchQueryBuilder builder, OperationContext context, List<EntitySpec> entities) {
    return find(builder.buildQuery(context, entities, "orders", true), TermQueryBuilder.class)
        .stream()
        .map(TermQueryBuilder::fieldName)
        .collect(Collectors.toSet());
  }

  private static OperationContext withFieldConfiguration(String label) {
    return OP_CONTEXT.withSearchFlags(flags -> flags.setFieldConfiguration(label));
  }

  @Test
  public void testFullTextFollowsDeclaredLabelSubfields() {
    Set<String> names =
        builder(null).getStandardFields(OP_CONTEXT, DATASET).stream()
            .map(SearchFieldConfig::fieldName)
            .collect(Collectors.toSet());
    assertTrue(names.contains("name.text"), names.toString());
    assertTrue(names.contains("name.stemmed"), names.toString());
    assertTrue(names.contains("description.text"), names.toString());
    assertFalse(names.contains("urnName.text"), names.toString());
    assertFalse(names.contains("urnName.stemmed"), names.toString());
    assertTrue(names.contains("qualifiedName.text"), names.toString());
    assertTrue(names.contains("glossaryTerms.text"), names.toString());
    assertTrue(names.contains("other.text"), names.toString());
    assertFalse(names.contains("platform.text"), names.toString());
    assertTrue(names.contains("tags.text"), names.toString());
    assertFalse(names.contains("platform.urnName.text"), names.toString());
    assertFalse(names.contains("tags.urnName.text"), names.toString());
    assertFalse(names.stream().anyMatch(name -> name.endsWith(".ngram")), names.toString());
    assertTrue(
        names.stream()
            .filter(name -> name.startsWith("_aspects"))
            .allMatch(name -> name.contains("__urn_name")),
        names.toString());
  }

  private static <T extends QueryBuilder> List<T> find(QueryBuilder query, Class<T> type) {
    List<T> found = new ArrayList<>();
    if (type.isInstance(query)) {
      found.add(type.cast(query));
    } else if (query instanceof FunctionScoreQueryBuilder functionScore) {
      found.addAll(find(functionScore.query(), type));
    } else if (query instanceof DisMaxQueryBuilder disMax) {
      disMax.innerQueries().forEach(clause -> found.addAll(find(clause, type)));
    } else if (query instanceof ConstantScoreQueryBuilder constantScore) {
      found.addAll(find(constantScore.innerQuery(), type));
    } else if (query instanceof BoolQueryBuilder bool) {
      for (List<QueryBuilder> clauses :
          List.of(bool.must(), bool.should(), bool.filter(), bool.mustNot())) {
        clauses.forEach(clause -> found.addAll(find(clause, type)));
      }
    }
    return found;
  }

  private static List<QueryStringQueryBuilder> queryStrings(QueryBuilder query) {
    return find(query, QueryStringQueryBuilder.class);
  }

  /** Normalized term an opted-in keyword writes for an unquoted structured value. */
  private static TermQueryBuilder keywordTerm(QueryBuilder query, String field, String value) {
    return only(
        find(query, TermQueryBuilder.class).stream()
            .filter(
                term ->
                    field.equals(term.fieldName())
                        && value.equals(term.value())
                        && !term.caseInsensitive())
            .toList());
  }

  private static <T> T only(List<T> found) {
    assertEquals(found.size(), 1, found.toString());
    return found.get(0);
  }
}
