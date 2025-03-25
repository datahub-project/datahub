package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.LongMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.FilterValue;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AggregationQueryBuilderTest {

  private static CachingAspectRetriever aspectRetriever;
  private static CachingAspectRetriever aspectRetrieverV1;
  private static String DEFAULT_FILTER = "_index";

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    Urn helloUrn = Urn.createFromString("urn:li:structuredProperty:hello");
    Urn abFghTenUrn = Urn.createFromString("urn:li:structuredProperty:ab.fgh.ten");
    Urn underscoresAndDotsUrn =
        Urn.createFromString("urn:li:structuredProperty:under.scores.and.dots_make_a_mess");

    // legacy
    aspectRetriever = mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    StructuredPropertyDefinition structPropHelloDefinition = new StructuredPropertyDefinition();
    structPropHelloDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    structPropHelloDefinition.setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropHelloDefinition.setQualifiedName("hello");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(helloUrn)), anySet()))
        .thenReturn(
            Map.of(
                helloUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropHelloDefinition.data()))));

    StructuredPropertyDefinition structPropAbFghTenDefinition = new StructuredPropertyDefinition();
    structPropAbFghTenDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    structPropAbFghTenDefinition.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropAbFghTenDefinition.setQualifiedName("ab.fgh.ten");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(abFghTenUrn)), anySet()))
        .thenReturn(
            Map.of(
                abFghTenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropAbFghTenDefinition.data()))));

    StructuredPropertyDefinition structPropUnderscoresAndDotsDefinition =
        new StructuredPropertyDefinition();
    structPropUnderscoresAndDotsDefinition.setVersion(null, SetMode.REMOVE_IF_NULL);
    structPropUnderscoresAndDotsDefinition.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropUnderscoresAndDotsDefinition.setQualifiedName("under.scores.and.dots_make_a_mess");
    structPropUnderscoresAndDotsDefinition.setDisplayName("under.scores.and.dots_make_a_mess");
    when(aspectRetriever.getLatestAspectObjects(eq(Set.of(underscoresAndDotsUrn)), anySet()))
        .thenReturn(
            Map.of(
                underscoresAndDotsUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropUnderscoresAndDotsDefinition.data()))));

    // V1
    aspectRetrieverV1 = mock(CachingAspectRetriever.class);
    when(aspectRetrieverV1.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());

    StructuredPropertyDefinition structPropHelloDefinitionV1 = new StructuredPropertyDefinition();
    structPropHelloDefinitionV1.setVersion("00000000000001");
    structPropHelloDefinitionV1.setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropHelloDefinitionV1.setQualifiedName("hello");
    when(aspectRetrieverV1.getLatestAspectObjects(eq(Set.of(helloUrn)), anySet()))
        .thenReturn(
            Map.of(
                helloUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropHelloDefinitionV1.data()))));

    StructuredPropertyDefinition structPropAbFghTenDefinitionV1 =
        new StructuredPropertyDefinition();
    structPropAbFghTenDefinitionV1.setVersion("00000000000001");
    structPropAbFghTenDefinitionV1.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropAbFghTenDefinitionV1.setQualifiedName("ab.fgh.ten");
    when(aspectRetrieverV1.getLatestAspectObjects(eq(Set.of(abFghTenUrn)), anySet()))
        .thenReturn(
            Map.of(
                abFghTenUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropAbFghTenDefinitionV1.data()))));

    StructuredPropertyDefinition structPropUnderscoresAndDotsDefinitionV1 =
        new StructuredPropertyDefinition();
    structPropUnderscoresAndDotsDefinitionV1.setVersion("00000000000001");
    structPropUnderscoresAndDotsDefinitionV1.setValueType(
        Urn.createFromString(DATA_TYPE_URN_PREFIX + "string"));
    structPropUnderscoresAndDotsDefinitionV1.setQualifiedName("under.scores.and.dots_make_a_mess");
    structPropUnderscoresAndDotsDefinitionV1.setDisplayName("under.scores.and.dots_make_a_mess");
    when(aspectRetrieverV1.getLatestAspectObjects(eq(Set.of(underscoresAndDotsUrn)), anySet()))
        .thenReturn(
            Map.of(
                underscoresAndDotsUrn,
                Map.of(
                    STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
                    new Aspect(structPropUnderscoresAndDotsDefinitionV1.data()))));
  }

  @Test
  public void testGetDefaultAggregationsHasFields() {
    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            true,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("hasTest")));
  }

  @Test
  public void testGetDefaultAggregationsFields() {

    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            true,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("test")));
  }

  @Test
  public void testGetSpecificAggregationsHasFields() {
    SearchableAnnotation annotation1 =
        new SearchableAnnotation(
            "test1",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest1"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchableAnnotation annotation2 =
        new SearchableAnnotation(
            "test2",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config,
            ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation1, annotation2)));

    // Case 1: Ask for fields that should exist.
    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of("test1", "test2", "hasTest1"));
    Assert.assertEquals(aggs.size(), 5);
    Set<String> facets = aggs.stream().map(AggregationBuilder::getName).collect(Collectors.toSet());
    Assert.assertEquals(
        ImmutableSet.of("test1", "test2", "hasTest1", "_entityType", "_entityType␞typeNames"),
        facets);

    // Case 2: Ask for fields that should NOT exist.
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of("hasTest2"));
    Assert.assertEquals(
        aggs.size(), 2); // default has two fields already, hasTest2 will not be in there
  }

  @Test
  public void testAggregateOverStructuredProperty() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever),
            List.of("structuredProperties.ab.fgh.ten"));
    Assert.assertEquals(aggs.size(), 3);
    AggregationBuilder aggBuilder = aggs.get(0);
    Assert.assertTrue(aggBuilder instanceof TermsAggregationBuilder);
    TermsAggregationBuilder agg = (TermsAggregationBuilder) aggBuilder;
    // Check that field name is sanitized to correct field name
    Assert.assertEquals(
        agg.field(),
        "structuredProperties.ab_fgh_ten.keyword",
        "Terms aggregate must be on a keyword or subfield keyword");

    // Two structured properties
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever),
            List.of("structuredProperties.ab.fgh.ten", "structuredProperties.hello"));
    Assert.assertEquals(
        aggs.size(),
        4); // has two default filters (_entityType, _entityType␞typeNames) both get mapped to
    // _index
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties.ab_fgh_ten.keyword",
            "structuredProperties.hello.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testAggregateOverStructuredPropertyNamespaced() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever),
            List.of("structuredProperties.under.scores.and.dots_make_a_mess"));
    Assert.assertEquals(aggs.size(), 3);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of("structuredProperties.under_scores_and_dots_make_a_mess.keyword", DEFAULT_FILTER));
    // Two structured properties
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever),
            List.of(
                "structuredProperties.under.scores.and.dots_make_a_mess",
                "structuredProperties.hello"));
    Assert.assertEquals(aggs.size(), 4);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties.under_scores_and_dots_make_a_mess.keyword",
            "structuredProperties.hello.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testAggregateOverStructuredPropertyV1() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetrieverV1),
            List.of("structuredProperties.ab.fgh.ten"));
    Assert.assertEquals(aggs.size(), 3);
    AggregationBuilder aggBuilder = aggs.get(0);
    Assert.assertTrue(aggBuilder instanceof TermsAggregationBuilder);
    TermsAggregationBuilder agg = (TermsAggregationBuilder) aggBuilder;
    // Check that field name is sanitized to correct field name
    Assert.assertEquals(
        agg.field(),
        "structuredProperties._versioned.ab_fgh_ten.00000000000001.string.keyword",
        "Terms aggregation must be on a keyword field or subfield.");

    // Two structured properties
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetrieverV1),
            List.of(
                "structuredProperties.ab.fgh.ten",
                "structuredProperties._versioned.hello.00000000000001.string"));
    Assert.assertEquals(
        aggs.size(),
        4); // has two default filters (_entityType, _entityType␞typeNames) both get mapped to
    // _index
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties._versioned.ab_fgh_ten.00000000000001.string.keyword",
            "structuredProperties._versioned.hello.00000000000001.string.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testAggregateOverStructuredPropertyNamespacedV1() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetrieverV1),
            List.of("structuredProperties.under.scores.and.dots_make_a_mess"));
    Assert.assertEquals(aggs.size(), 3);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties._versioned.under_scores_and_dots_make_a_mess.00000000000001.string.keyword",
            DEFAULT_FILTER));

    // Two structured properties
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetrieverV1),
            List.of(
                "structuredProperties.under.scores.and.dots_make_a_mess",
                "structuredProperties._versioned.hello.00000000000001.string"));
    Assert.assertEquals(aggs.size(), 4);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties._versioned.under_scores_and_dots_make_a_mess.00000000000001.string.keyword",
            "structuredProperties._versioned.hello.00000000000001.string.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testAggregateOverFieldsAndStructProp() {
    SearchableAnnotation annotation1 =
        new SearchableAnnotation(
            "test1",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest1"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchableAnnotation annotation2 =
        new SearchableAnnotation(
            "test2",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config,
            ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation1, annotation2)));

    // Aggregate over fields and structured properties
    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever),
            ImmutableList.of(
                "test1",
                "test2",
                "hasTest1",
                "structuredProperties.ab.fgh.ten",
                "structuredProperties.hello"));
    Assert.assertEquals(aggs.size(), 7);
    Set<String> facets =
        aggs.stream()
            .map(aggB -> ((TermsAggregationBuilder) aggB).field())
            .collect(Collectors.toSet());
    Assert.assertEquals(
        facets,
        ImmutableSet.of(
            "test1.keyword",
            "test2.keyword",
            "hasTest1",
            "structuredProperties.ab_fgh_ten.keyword",
            "structuredProperties.hello.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testAggregateOverFieldsAndStructPropV1() {
    SearchableAnnotation annotation1 =
        new SearchableAnnotation(
            "test1",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest1"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchableAnnotation annotation2 =
        new SearchableAnnotation(
            "test2",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            false,
            Optional.of("Test Filter"),
            Optional.empty(),
            1.0,
            Optional.empty(),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            false,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config,
            ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation1, annotation2)));

    // Aggregate over fields and structured properties
    List<AggregationBuilder> aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(aspectRetrieverV1),
            ImmutableList.of(
                "test1",
                "test2",
                "hasTest1",
                "structuredProperties.ab.fgh.ten",
                "structuredProperties.hello"));
    Assert.assertEquals(
        aggs.size(),
        7); // has two default filters (_entityType, _entityType␞typeNames) both get mapped to
    // _index
    Set<String> facets =
        aggs.stream()
            .map(aggB -> ((TermsAggregationBuilder) aggB).field())
            .collect(Collectors.toSet());
    Assert.assertEquals(
        facets,
        ImmutableSet.of(
            "test1.keyword",
            "test2.keyword",
            "hasTest1",
            "structuredProperties._versioned.ab_fgh_ten.00000000000001.string.keyword",
            "structuredProperties._versioned.hello.00000000000001.string.keyword",
            DEFAULT_FILTER));
  }

  @Test
  public void testMissingAggregation() {

    SearchableAnnotation annotation =
        new SearchableAnnotation(
            "test",
            SearchableAnnotation.FieldType.KEYWORD,
            true,
            true,
            false,
            true,
            Optional.empty(),
            Optional.of("Has Test"),
            1.0,
            Optional.of("hasTest"),
            Optional.empty(),
            Collections.emptyMap(),
            Collections.emptyList(),
            true,
            false,
            Optional.empty());

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of(annotation)));

    List<AggregationBuilder> aggs =
        builder.getAggregations(TestOperationContexts.systemContextNoSearchAuthorization());

    Assert.assertTrue(aggs.stream().anyMatch(agg -> agg.getName().equals("hasTest")));
    Assert.assertTrue(
        aggs.stream()
            .anyMatch(
                agg ->
                    agg.getName()
                        .equals(
                            MISSING_SPECIAL_TYPE + AGGREGATION_SPECIAL_TYPE_DELIMITER + "test")));
  }

  @Test
  public void testUpdateAggregationEntityWithStructuredProp() {
    final AggregationMetadata aggregationMetadata = new AggregationMetadata();
    aggregationMetadata.setName("structuredProperties.test_me.one");

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    builder.updateAggregationEntity(aggregationMetadata);
    Assert.assertEquals(
        aggregationMetadata.getEntity(), UrnUtils.getUrn("urn:li:structuredProperty:test_me.one"));
  }

  @Test
  public void testUpdateAggregationEntityWithRegularFilter() {
    final AggregationMetadata aggregationMetadata = new AggregationMetadata();
    aggregationMetadata.setName("domains");

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    builder.updateAggregationEntity(aggregationMetadata);
    Assert.assertNull(aggregationMetadata.getEntity());
  }

  @Test
  public void testAddFiltersToMetadataWithStructuredPropsNoResults() {
    final Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test_me.one");

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    Criterion criterion =
        new Criterion()
            .setField("structuredProperties.test_me.one")
            .setValues(new StringArray("test123"))
            .setCondition(Condition.EQUAL);

    AspectRetriever mockAspectRetriever = getMockAspectRetriever(propertyUrn);

    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();
    builder.addCriterionFiltersToAggregationMetadata(
        criterion, aggregationMetadataList, mockAspectRetriever);

    // ensure we add the correct structured prop aggregation here
    Assert.assertEquals(aggregationMetadataList.size(), 1);
    Assert.assertEquals(aggregationMetadataList.get(0).getEntity(), propertyUrn);
    Assert.assertEquals(
        aggregationMetadataList.get(0).getName(), "structuredProperties.test_me.one");
    Assert.assertEquals(aggregationMetadataList.get(0).getAggregations().size(), 1);
    Assert.assertEquals(aggregationMetadataList.get(0).getAggregations().get("test123"), 0);
  }

  @Test
  public void testAddFiltersToMetadataWithStructuredPropsWithAggregations() {
    final Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test_me.one");

    final AggregationMetadata aggregationMetadata = new AggregationMetadata();
    aggregationMetadata.setName("structuredProperties.test_me.one");
    aggregationMetadata.setEntity(propertyUrn);
    FilterValue filterValue =
        new FilterValue().setValue("test123").setFiltered(false).setFacetCount(1);
    aggregationMetadata.setFilterValues(new FilterValueArray(filterValue));
    LongMap aggregations = new LongMap();
    aggregations.put("test123", 1L);
    aggregationMetadata.setAggregations(aggregations);

    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(25);

    AggregationQueryBuilder builder =
        new AggregationQueryBuilder(
            config, ImmutableMap.of(mock(EntitySpec.class), ImmutableList.of()));

    Criterion criterion =
        new Criterion()
            .setField("structuredProperties.test_me.one")
            .setValues(new StringArray("test123"))
            .setCondition(Condition.EQUAL);

    AspectRetriever mockAspectRetriever = getMockAspectRetriever(propertyUrn);

    final List<AggregationMetadata> aggregationMetadataList = new ArrayList<>();
    aggregationMetadataList.add(aggregationMetadata);
    builder.addCriterionFiltersToAggregationMetadata(
        criterion, aggregationMetadataList, mockAspectRetriever);

    Assert.assertEquals(aggregationMetadataList.size(), 1);
    Assert.assertEquals(aggregationMetadataList.get(0).getEntity(), propertyUrn);
    Assert.assertEquals(
        aggregationMetadataList.get(0).getName(), "structuredProperties.test_me.one");
    Assert.assertEquals(aggregationMetadataList.get(0).getAggregations().size(), 1);
    Assert.assertEquals(aggregationMetadataList.get(0).getAggregations().get("test123"), 1);
  }

  private AspectRetriever getMockAspectRetriever(Urn propertyUrn) {
    AspectRetriever mockAspectRetriever = Mockito.mock(AspectRetriever.class);
    Map<Urn, Map<String, Aspect>> mockResult = new HashMap<>();
    Map<String, Aspect> aspectMap = new HashMap<>();
    DataMap definition = new DataMap();
    definition.put("qualifiedName", "test_me.one");
    definition.put("valueType", "urn:li:dataType:datahub.string");
    Aspect definitionAspect = new Aspect(definition);
    aspectMap.put(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, definitionAspect);
    mockResult.put(propertyUrn, aspectMap);
    Set<Urn> urns = new HashSet<>();
    urns.add(propertyUrn);
    Mockito.when(mockAspectRetriever.getLatestAspectObjects(eq(urns), any()))
        .thenReturn(mockResult);

    return mockAspectRetriever;
  }
}
