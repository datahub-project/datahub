package com.linkedin.metadata.search.query.request;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.utils.SearchUtil.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.AggregationQueryBuilder;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AggregationQueryBuilderTest {

  private static AspectRetriever aspectRetriever;
  private static AspectRetriever aspectRetrieverV1;

  @BeforeClass
  public static void setup() throws RemoteInvocationException, URISyntaxException {
    Urn helloUrn = Urn.createFromString("urn:li:structuredProperty:hello");
    Urn abFghTenUrn = Urn.createFromString("urn:li:structuredProperty:ab.fgh.ten");

    // legacy
    aspectRetriever = mock(AspectRetriever.class);
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

    // V1
    aspectRetrieverV1 = mock(AspectRetriever.class);
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
            false);

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
            false);

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
            false);

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
            false);

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
    Assert.assertEquals(aggs.size(), 3);
    Set<String> facets = aggs.stream().map(AggregationBuilder::getName).collect(Collectors.toSet());
    Assert.assertEquals(ImmutableSet.of("test1", "test2", "hasTest1"), facets);

    // Case 2: Ask for fields that should NOT exist.
    aggs =
        builder.getAggregations(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            ImmutableList.of("hasTest2"));
    Assert.assertEquals(aggs.size(), 0);
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
    Assert.assertEquals(aggs.size(), 1);
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
    Assert.assertEquals(aggs.size(), 2);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of("structuredProperties.ab_fgh_ten.keyword", "structuredProperties.hello.keyword"));
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
    Assert.assertEquals(aggs.size(), 1);
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
    Assert.assertEquals(aggs.size(), 2);
    Assert.assertEquals(
        aggs.stream()
            .map(aggr -> ((TermsAggregationBuilder) aggr).field())
            .collect(Collectors.toSet()),
        Set.of(
            "structuredProperties._versioned.ab_fgh_ten.00000000000001.string.keyword",
            "structuredProperties._versioned.hello.00000000000001.string.keyword"));
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
            false);

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
            false);

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
    Assert.assertEquals(aggs.size(), 5);
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
            "structuredProperties.hello.keyword"));
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
            false);

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
            false);

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
    Assert.assertEquals(aggs.size(), 5);
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
            "structuredProperties._versioned.hello.00000000000001.string.keyword"));
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
            true);

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
}
