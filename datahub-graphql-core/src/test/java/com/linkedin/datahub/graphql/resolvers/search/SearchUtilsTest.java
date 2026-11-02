package com.linkedin.datahub.graphql.resolvers.search;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchUtilsTest {

  @Test
  public static void testApplyViewToEntityTypes() {

    List<String> baseEntityTypes =
        ImmutableList.of(Constants.CHART_ENTITY_NAME, Constants.DATASET_ENTITY_NAME);

    List<String> viewEntityTypes =
        ImmutableList.of(Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME);

    final List<String> result = SearchUtils.intersectEntityTypes(baseEntityTypes, viewEntityTypes);

    final List<String> expectedResult = ImmutableList.of(Constants.DATASET_ENTITY_NAME);
    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public void testGetSearchEntityNamesExplicitTypesBypassConfig() {
    List<String> explicit = SearchUtils.getSearchEntityNames(null, List.of(EntityType.DATASET));
    Assert.assertEquals(explicit, List.of("dataset"));
  }

  @Test
  public void testGetSearchEntityNamesEmptyConfiguredDefaultsReturnsEmpty() {
    SearchContext searchContext =
        SearchContext.EMPTY.toBuilder().defaultSearchEntityNames(List.of()).build();
    OperationContext opContext = Mockito.mock(OperationContext.class);
    Mockito.when(opContext.getSearchContext()).thenReturn(searchContext);

    Assert.assertEquals(SearchUtils.getSearchEntityNames(opContext, null), List.of());
    Assert.assertEquals(SearchUtils.getSearchEntityNames(opContext, List.of()), List.of());
  }

  @Test
  public void testCreateEmptyScrollResults() {
    var empty = SearchUtils.createEmptyScrollResults(25);
    Assert.assertEquals(empty.getCount(), Integer.valueOf(25));
    Assert.assertEquals(empty.getTotal(), Integer.valueOf(0));
    Assert.assertTrue(empty.getSearchResults().isEmpty());
  }

  @Test
  public static void testStructuredPropertyAppliesToEntityTypes() {
    ObjectMapper mapper = new ObjectMapper();
    List<String> searched = ImmutableList.of("glossaryTerm");

    // Excluded: property scoped to a different entity type
    Assert.assertFalse(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:datahub.dataset\"]", searched));

    // Kept: matching entity type (case-insensitive against the searched names)
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:datahub.glossaryTerm\"]", searched));
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:datahub.GLOSSARYTERM\"]", searched));

    // Kept: any one of multiple types matching is enough
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper,
            "[\"urn:li:entityType:datahub.dataset\",\"urn:li:entityType:datahub.glossaryTerm\"]",
            searched));

    // Kept: empty searched entity list means all types
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:datahub.dataset\"]", ImmutableList.of()));

    // Legacy entity-type URN form (no datahub. prefix) resolves the same way, so an in-scope
    // property is still kept and an out-of-scope one still excluded.
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:glossaryTerm\"]", searched));
    Assert.assertFalse(
        SearchUtils.structuredPropertyAppliesToEntityTypes(
            mapper, "[\"urn:li:entityType:dataset\"]", searched));
  }

  @Test
  public static void testStructuredPropertyAppliesToEntityTypesFailsOpen() {
    ObjectMapper mapper = new ObjectMapper();
    List<String> searched = ImmutableList.of("glossaryTerm");

    // Missing, empty, or unparseable entityTypes keep the property
    Assert.assertTrue(SearchUtils.structuredPropertyAppliesToEntityTypes(mapper, null, searched));
    Assert.assertTrue(SearchUtils.structuredPropertyAppliesToEntityTypes(mapper, "[]", searched));
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(mapper, "not json", searched));
    // A malformed (null) entry is kept rather than silently dropped
    Assert.assertTrue(
        SearchUtils.structuredPropertyAppliesToEntityTypes(mapper, "[null]", searched));
  }

  private SearchUtilsTest() {}
}
