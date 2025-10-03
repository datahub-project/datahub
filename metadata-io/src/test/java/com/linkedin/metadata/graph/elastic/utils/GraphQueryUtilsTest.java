package com.linkedin.metadata.graph.elastic.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.GraphQueryConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.ThreadSafePathStore;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphQueryUtilsTest {

  @Mock private OperationContext mockOpContext;

  @Mock private SearchContext mockSearchContext;

  @Mock private SearchFlags mockSearchFlags;

  @Mock private SearchResponse mockSearchResponse;

  @Mock private SearchHits mockSearchHits;

  @Mock private SearchHit mockSearchHit;

  private static final Urn TEST_URN_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset_1,PROD)");
  private static final Urn TEST_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset_2,PROD)");
  private static final Urn TEST_URN_3 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test_dataset_3,PROD)");
  private static final Urn TEST_URN_4 = UrnUtils.getUrn("urn:li:dataPlatform:snowflake");

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup mock behavior
    org.mockito.Mockito.when(mockOpContext.getSearchContext()).thenReturn(mockSearchContext);
    org.mockito.Mockito.when(mockSearchContext.getSearchFlags()).thenReturn(mockSearchFlags);
    org.mockito.Mockito.when(mockSearchFlags.isIncludeSoftDeleted()).thenReturn(false);
  }

  @Test
  public void testAddFilterToQueryBuilder() {
    Filter filter = new Filter();
    ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    Criterion criterion = new Criterion();
    criterion.setField("testField");
    criterion.setCondition(Condition.EQUAL);
    criterion.setValues(new StringArray("value1", "value2"));
    conjunction.setAnd(new CriterionArray(criterion));
    filter.setOr(new ConjunctiveCriterionArray(conjunction));

    BoolQueryBuilder rootQuery = new BoolQueryBuilder();

    GraphQueryUtils.addFilterToQueryBuilder(filter, "testNode", rootQuery);

    assertNotNull(rootQuery.filter());
    assertEquals(rootQuery.filter().size(), 1);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testAddFilterToQueryBuilderUnsupportedCondition() {
    Filter filter = new Filter();
    ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    Criterion criterion = new Criterion();
    criterion.setField("testField");
    criterion.setCondition(Condition.GREATER_THAN); // Unsupported condition
    criterion.setValues(new StringArray("value1"));
    conjunction.setAnd(new CriterionArray(criterion));
    filter.setOr(new ConjunctiveCriterionArray(conjunction));

    BoolQueryBuilder rootQuery = new BoolQueryBuilder();

    GraphQueryUtils.addFilterToQueryBuilder(filter, "testNode", rootQuery);
  }

  @Test
  public void testBuildQueryBasic() {
    GraphQueryConfiguration config = new GraphQueryConfiguration();
    config.setGraphStatusEnabled(true);

    Filter sourceFilter = new Filter();
    sourceFilter.setOr(new ConjunctiveCriterionArray());
    Filter destFilter = new Filter();
    destFilter.setOr(new ConjunctiveCriterionArray());
    Set<String> sourceTypes = new HashSet<>(Arrays.asList("dataset"));
    Set<String> destTypes = new HashSet<>(Arrays.asList("table"));
    Set<String> relationshipTypes = new HashSet<>(Arrays.asList("DownstreamOf"));
    RelationshipFilter relationshipFilter =
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);

    GraphFilters filters =
        new GraphFilters(
            sourceFilter,
            destFilter,
            sourceTypes,
            destTypes,
            relationshipTypes,
            relationshipFilter);

    BoolQueryBuilder result = GraphQueryUtils.buildQuery(mockOpContext, config, filters);

    assertNotNull(result);
    assertTrue(result.filter().size() > 0);
  }

  @Test
  public void testBuildQueryWithLifecycleOwner() {
    GraphQueryConfiguration config = new GraphQueryConfiguration();
    config.setGraphStatusEnabled(true);

    Filter sourceFilter = new Filter();
    sourceFilter.setOr(new ConjunctiveCriterionArray());
    Filter destFilter = new Filter();
    destFilter.setOr(new ConjunctiveCriterionArray());
    RelationshipFilter relationshipFilter =
        new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING);

    GraphFilters filters =
        new GraphFilters(sourceFilter, destFilter, null, null, new HashSet<>(), relationshipFilter);

    BoolQueryBuilder result =
        GraphQueryUtils.buildQuery(mockOpContext, config, filters, "testOwner");

    assertNotNull(result);
    assertTrue(result.filter().size() > 0);
  }

  @Test
  public void testContainsCycle() {
    UrnArray path = new UrnArray();
    path.add(TEST_URN_1);
    path.add(TEST_URN_2);
    path.add(TEST_URN_1); // Duplicate - creates cycle

    assertTrue(GraphQueryUtils.containsCycle(path));

    UrnArray noCyclePath = new UrnArray();
    noCyclePath.add(TEST_URN_1);
    noCyclePath.add(TEST_URN_2);
    noCyclePath.add(TEST_URN_3);

    assertFalse(GraphQueryUtils.containsCycle(noCyclePath));
  }

  @Test
  public void testPlatformMatches() {
    UrnArray platforms = new UrnArray();
    platforms.add(TEST_URN_4);

    assertTrue(GraphQueryUtils.platformMatches(TEST_URN_1, platforms));
    assertFalse(GraphQueryUtils.platformMatches(TEST_URN_4, new UrnArray()));
  }

  @Test
  public void testClonePath() {
    UrnArray originalPath = new UrnArray();
    originalPath.add(TEST_URN_1);
    originalPath.add(TEST_URN_2);

    UrnArray clonedPath = GraphQueryUtils.clonePath(originalPath);

    assertNotNull(clonedPath);
    assertEquals(clonedPath.size(), originalPath.size());
    assertEquals(clonedPath.get(0), originalPath.get(0));
    assertEquals(clonedPath.get(1), originalPath.get(1));
    assertFalse(clonedPath == originalPath); // Different objects
  }

  @Test
  public void testGetViaPaths() {
    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    // Add some test paths
    UrnArray path1 = new UrnArray();
    path1.add(TEST_URN_1);
    path1.add(TEST_URN_2);
    path1.add(TEST_URN_3);
    pathStore.addPath(TEST_URN_3, path1);

    UrnArray path2 = new UrnArray();
    path2.add(TEST_URN_1);
    path2.add(TEST_URN_2);
    pathStore.addPath(TEST_URN_2, path2);

    UrnArrayArray viaPaths = GraphQueryUtils.getViaPaths(pathStore, TEST_URN_3, TEST_URN_2);

    assertNotNull(viaPaths);
    assertEquals(viaPaths.size(), 1);

    UrnArray viaPath = viaPaths.get(0);
    assertEquals(viaPath.size(), 2);
    assertEquals(viaPath.get(0), TEST_URN_1);
    assertEquals(viaPath.get(1), TEST_URN_2);
  }

  @Test
  public void testExtractViaEntity() {
    Map<String, Object> document = new HashMap<>();
    document.put("via", TEST_URN_1.toString());

    Urn result = GraphQueryUtils.extractViaEntity(document);

    assertNotNull(result);
    assertEquals(result, TEST_URN_1);
  }

  @Test
  public void testExtractViaEntityNull() {
    Map<String, Object> document = new HashMap<>();

    Urn result = GraphQueryUtils.extractViaEntity(document);

    assertNull(result);
  }

  @Test
  public void testExtractViaEntityInvalidUrn() {
    Map<String, Object> document = new HashMap<>();
    document.put("via", "invalid-urn");

    Urn result = GraphQueryUtils.extractViaEntity(document);

    assertNull(result);
  }

  @Test
  public void testExtractCreatedOn() {
    Map<String, Object> document = new HashMap<>();
    document.put("createdOn", 1234567890L);

    Long result = GraphQueryUtils.extractCreatedOn(document);

    assertEquals(result, Long.valueOf(1234567890L));
  }

  @Test
  public void testExtractCreatedOnNull() {
    Map<String, Object> document = new HashMap<>();

    Long result = GraphQueryUtils.extractCreatedOn(document);

    assertNull(result);
  }

  @Test
  public void testExtractCreatedActor() {
    Map<String, Object> document = new HashMap<>();
    document.put("createdActor", TEST_URN_1.toString());

    Urn result = GraphQueryUtils.extractCreatedActor(document);

    assertNotNull(result);
    assertEquals(result, TEST_URN_1);
  }

  @Test
  public void testExtractCreatedActorNull() {
    Map<String, Object> document = new HashMap<>();

    Urn result = GraphQueryUtils.extractCreatedActor(document);

    assertNull(result);
  }

  @Test
  public void testExtractUpdatedOn() {
    Map<String, Object> document = new HashMap<>();
    document.put("updatedOn", 1234567890L);

    Long result = GraphQueryUtils.extractUpdatedOn(document);

    assertEquals(result, Long.valueOf(1234567890L));
  }

  @Test
  public void testExtractUpdatedOnNull() {
    Map<String, Object> document = new HashMap<>();

    Long result = GraphQueryUtils.extractUpdatedOn(document);

    assertNull(result);
  }

  @Test
  public void testExtractUpdatedActor() {
    Map<String, Object> document = new HashMap<>();
    document.put("updatedActor", TEST_URN_1.toString());

    Urn result = GraphQueryUtils.extractUpdatedActor(document);

    assertNotNull(result);
    assertEquals(result, TEST_URN_1);
  }

  @Test
  public void testExtractUpdatedActorNull() {
    Map<String, Object> document = new HashMap<>();

    Urn result = GraphQueryUtils.extractUpdatedActor(document);

    assertNull(result);
  }

  @Test
  public void testIsManual() {
    Map<String, Object> document = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    properties.put("source", "UI");
    document.put("properties", properties);

    assertTrue(GraphQueryUtils.isManual(document));
  }

  @Test
  public void testIsManualFalse() {
    Map<String, Object> document = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    properties.put("source", "API");
    document.put("properties", properties);

    assertFalse(GraphQueryUtils.isManual(document));
  }

  @Test
  public void testIsManualNoProperties() {
    Map<String, Object> document = new HashMap<>();

    assertFalse(GraphQueryUtils.isManual(document));
  }

  @Test
  public void testIsRelationshipConnectedToInput() {
    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    // Add a path that contains the input URN
    UrnArray path = new UrnArray();
    path.add(TEST_URN_1);
    path.add(TEST_URN_2);
    pathStore.addPath(TEST_URN_2, path);

    LineageRelationship relationship = new LineageRelationship();
    relationship.setEntity(TEST_URN_2);

    assertTrue(GraphQueryUtils.isRelationshipConnectedToInput(relationship, TEST_URN_1, pathStore));
    assertFalse(
        GraphQueryUtils.isRelationshipConnectedToInput(relationship, TEST_URN_3, pathStore));
  }

  @Test
  public void testAddEdgeToPaths() {
    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    // Add initial path to parent
    UrnArray parentPath = new UrnArray();
    parentPath.add(TEST_URN_1);
    pathStore.addPath(TEST_URN_1, parentPath);

    boolean result = GraphQueryUtils.addEdgeToPaths(pathStore, TEST_URN_1, null, TEST_URN_2);

    assertTrue(result);

    Set<UrnArray> pathsToChild = pathStore.getPaths(TEST_URN_2);
    assertNotNull(pathsToChild);
    assertEquals(pathsToChild.size(), 1);

    UrnArray childPath = pathsToChild.iterator().next();
    assertEquals(childPath.size(), 2);
    assertEquals(childPath.get(0), TEST_URN_1);
    assertEquals(childPath.get(1), TEST_URN_2);
  }

  @Test
  public void testAddEdgeToPathsWithVia() {
    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    // Add initial path to parent
    UrnArray parentPath = new UrnArray();
    parentPath.add(TEST_URN_1);
    pathStore.addPath(TEST_URN_1, parentPath);

    boolean result = GraphQueryUtils.addEdgeToPaths(pathStore, TEST_URN_1, TEST_URN_3, TEST_URN_2);

    assertTrue(result);

    Set<UrnArray> pathsToChild = pathStore.getPaths(TEST_URN_2);
    assertNotNull(pathsToChild);
    assertEquals(pathsToChild.size(), 1);

    UrnArray childPath = pathsToChild.iterator().next();
    assertEquals(childPath.size(), 3);
    assertEquals(childPath.get(0), TEST_URN_1);
    assertEquals(childPath.get(1), TEST_URN_3);
    assertEquals(childPath.get(2), TEST_URN_2);
  }

  @Test
  public void testAddEdgeToPathsNoExistingPaths() {
    ThreadSafePathStore pathStore = new ThreadSafePathStore();

    boolean result = GraphQueryUtils.addEdgeToPaths(pathStore, TEST_URN_1, null, TEST_URN_2);

    assertTrue(result);

    Set<UrnArray> pathsToChild = pathStore.getPaths(TEST_URN_2);
    assertNotNull(pathsToChild);
    assertEquals(pathsToChild.size(), 1);

    UrnArray childPath = pathsToChild.iterator().next();
    assertEquals(childPath.size(), 2);
    assertEquals(childPath.get(0), TEST_URN_1);
    assertEquals(childPath.get(1), TEST_URN_2);
  }
}
