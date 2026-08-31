package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Triple;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for ESSearchDAO to verify that duplicate entity names are handled correctly.
 *
 * <p>This test verifies the fix for the bug where duplicate entity names would cause
 * IllegalStateException: Duplicate key DefaultEntitySpec when collecting EntitySpec instances into
 * a Map in SearchRequestHandler.getSearchableAnnotations().
 */
public class ESSearchDAODuplicateEntityNamesTest {

  private ESSearchDAO esSearchDAO;
  private OperationContext opContext;
  private SearchClientShim<?> mockClient;

  @BeforeMethod
  public void setUp() {
    // Setup mocks
    mockClient = Mockito.mock(SearchClientShim.class);
    opContext = TestOperationContexts.systemContextNoValidate();

    // Create ESSearchDAO with mocked client
    esSearchDAO =
        new ESSearchDAO(
            mockClient,
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @Test
  public void testBuildSearchRequestWithDuplicateEntityNames() {
    // Given: Duplicate entity names (e.g., from UI state management issues)
    List<String> entityNames = Arrays.asList("dataset", "dataset", "dashboard");

    // When: Building search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildSearchRequest(opContext, entityNames, "*", null, null, 0, 10, List.of());

    // Then: Should deduplicate to unique EntitySpec instances
    assertNotNull(result);
    List<EntitySpec> entitySpecs = result.getRight();
    assertNotNull(entitySpecs);

    // Verify we have only 2 unique entity specs (dataset and dashboard)
    assertEquals(entitySpecs.size(), 2, "Should have 2 unique EntitySpec instances");

    // Verify all EntitySpec instances are unique (different object references)
    Set<EntitySpec> uniqueSpecs = new HashSet<>(entitySpecs);
    assertEquals(
        uniqueSpecs.size(), 2, "All EntitySpec instances should be unique (no duplicates)");

    // Verify we have both dataset and dashboard
    Set<String> entityNamesInSpecs =
        entitySpecs.stream().map(EntitySpec::getName).collect(java.util.stream.Collectors.toSet());
    assertTrue(entityNamesInSpecs.contains("dataset"), "Should contain dataset entity spec");
    assertTrue(entityNamesInSpecs.contains("dashboard"), "Should contain dashboard entity spec");
  }

  @Test
  public void testBuildSearchRequestWithMultipleDuplicates() {
    // Given: Multiple duplicates of the same entity type
    List<String> entityNames =
        Arrays.asList("dataset", "dataset", "dataset", "dashboard", "dashboard");

    // When: Building search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildSearchRequest(opContext, entityNames, "*", null, null, 0, 10, List.of());

    // Then: Should deduplicate to 2 unique EntitySpec instances
    assertNotNull(result);
    List<EntitySpec> entitySpecs = result.getRight();
    assertEquals(
        entitySpecs.size(),
        2,
        "Should have 2 unique EntitySpec instances despite multiple duplicates");

    // Verify all are unique
    Set<EntitySpec> uniqueSpecs = new HashSet<>(entitySpecs);
    assertEquals(uniqueSpecs.size(), 2, "All EntitySpec instances should be unique");
  }

  @Test
  public void testBuildAggregateByValueWithDuplicateEntityNames() {
    // Given: Duplicate entity names
    List<String> entityNames = Arrays.asList("dataset", "dataset", "chart");

    // When: Building aggregate by value request
    // The buildAggregateByValue method is @VisibleForTesting and takes a list of entity names
    // It internally uses the same pattern that was fixed
    try {
      org.opensearch.action.search.SearchRequest searchRequest =
          esSearchDAO.buildAggregateByValue(opContext, entityNames, "testField", null, 10);
      // If we get here, no exception was thrown - the fix is working
      assertNotNull(
          searchRequest, "buildAggregateByValue should handle duplicates without exception");
    } catch (IllegalStateException e) {
      if (e.getMessage() != null && e.getMessage().contains("Duplicate key")) {
        org.testng.Assert.fail(
            "buildAggregateByValue should not throw IllegalStateException for duplicate entity names",
            e);
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testBuildScrollRequestWithDuplicateEntityNames() {
    // Given: Duplicate entity names
    List<String> entityNames = Arrays.asList("dataset", "dataset", "dashboard");

    // When: Building scroll request
    // The buildScrollRequest method is @VisibleForTesting and takes a list of entity names
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildScrollRequest(
            opContext,
            opContext.getSearchContext().getIndexConvention(),
            null,
            null,
            entityNames,
            10,
            null,
            "*",
            null,
            List.of());

    // Then: Should deduplicate to unique EntitySpec instances
    assertNotNull(result, "buildScrollRequest should handle duplicates without exception");
    List<EntitySpec> entitySpecs = result.getRight();
    assertNotNull(entitySpecs);

    // Verify we have only 2 unique entity specs (dataset and dashboard)
    assertEquals(
        entitySpecs.size(), 2, "Should have 2 unique EntitySpec instances despite duplicates");

    // Verify all EntitySpec instances are unique
    Set<EntitySpec> uniqueSpecs = new HashSet<>(entitySpecs);
    assertEquals(
        uniqueSpecs.size(), 2, "All EntitySpec instances should be unique (no duplicates)");
  }

  @Test
  public void testBuildSearchRequestWithNoDuplicates() {
    // Given: No duplicate entity names (normal case)
    List<String> entityNames = Arrays.asList("dataset", "dashboard", "chart");

    // When: Building search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildSearchRequest(opContext, entityNames, "*", null, null, 0, 10, List.of());

    // Then: Should work normally and return all 3 entity specs
    assertNotNull(result);
    List<EntitySpec> entitySpecs = result.getRight();
    assertEquals(entitySpecs.size(), 3, "Should have 3 EntitySpec instances when no duplicates");

    // Verify all are unique
    Set<EntitySpec> uniqueSpecs = new HashSet<>(entitySpecs);
    assertEquals(uniqueSpecs.size(), 3, "All EntitySpec instances should be unique");
  }

  @Test
  public void testBuildSearchRequestWithSingleEntityType() {
    // Given: Single entity type (no duplicates possible)
    List<String> entityNames = Arrays.asList("dataset");

    // When: Building search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildSearchRequest(opContext, entityNames, "*", null, null, 0, 10, List.of());

    // Then: Should return single EntitySpec
    assertNotNull(result);
    List<EntitySpec> entitySpecs = result.getRight();
    assertEquals(entitySpecs.size(), 1, "Should have 1 EntitySpec instance for single entity type");
    assertEquals(entitySpecs.get(0).getName(), "dataset", "EntitySpec should be for dataset");
  }

  @Test
  public void testBuildSearchRequestWithEmptyList() {
    // Given: Empty entity names list
    List<String> entityNames = Arrays.asList();

    // When: Building search request
    Triple<SearchRequest, Filter, List<EntitySpec>> result =
        esSearchDAO.buildSearchRequest(opContext, entityNames, "*", null, null, 0, 10, List.of());

    // Then: Should return empty list
    assertNotNull(result);
    List<EntitySpec> entitySpecs = result.getRight();
    assertNotNull(entitySpecs);
    assertEquals(
        entitySpecs.size(), 0, "Should have 0 EntitySpec instances for empty entity names list");
  }
}
