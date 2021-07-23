package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.query.RelationshipFilter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.linkedin.metadata.dao.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Base class for testing any GraphService implementation.
 * Derive the test class from this base and get your GraphService implementation
 * tested with all these tests.
 *
 * You can add implementation specific tests in derived classes, or add general tests
 * here and have all existing implementations tested in the same way.
 *
 * Note this base class does not test GraphService.addEdge explicitly. This method is tested
 * indirectly by all tests via `getPopulatedGraphService` at the beginning of each test.
 * The `getPopulatedGraphService` method calls `GraphService.addEdge` to populate the Graph.
 * Feel free to add a test to your test implementation that calls `getPopulatedGraphService` and
 * asserts the state of the graph in an implementation specific way.
 */
abstract public class GraphServiceTestBase {

  /**
   * Some test URN types.
   */
  protected static String datasetType = "dataset";
  protected static String userType = "user";

  /**
   * Some test datasets.
   */
  protected static String datasetOneUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetOne,PROD)";
  protected static String datasetTwoUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetTwo,PROD)";
  protected static String datasetThreeUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetThree,PROD)";
  protected static String datasetFourUrnString = "urn:li:" + datasetType + ":(urn:li:dataPlatform:type,SampleDatasetFour,PROD)";

  protected static Urn datasetOneUrn = createFromString(datasetOneUrnString);
  protected static Urn datasetTwoUrn = createFromString(datasetTwoUrnString);
  protected static Urn datasetThreeUrn = createFromString(datasetThreeUrnString);
  protected static Urn datasetFourUrn = createFromString(datasetFourUrnString);

  /**
   * Some dataset owners.
   */
  protected static String userOneUrnString = "urn:li:" + userType + ":(urn:li:user:system,Ingress,PROD)";
  protected static String userTwoUrnString = "urn:li:" + userType + ":(urn:li:user:individual,UserA,DEV)";

  protected static Urn userOneUrn = createFromString(userOneUrnString);
  protected static Urn userTwoUrn = createFromString(userTwoUrnString);

  /**
   * Some test relationships.
   */
  protected static String downstreamOf = "DownstreamOf";
  protected static String hasOwner = "HasOwner";
  protected static String knowsUser = "KnowsUser";
  protected static Set<String> allRelationshipTypes = new HashSet<>(Arrays.asList(downstreamOf, hasOwner, knowsUser));

  /**
   * Some test related entities.
   */
  protected static RelatedEntity downstreamOfDatasetOneRelatedEntity = new RelatedEntity(downstreamOf, datasetOneUrnString);
  protected static RelatedEntity downstreamOfDatasetTwoRelatedEntity = new RelatedEntity(downstreamOf, datasetTwoUrnString);
  protected static RelatedEntity downstreamOfDatasetThreeRelatedEntity = new RelatedEntity(downstreamOf, datasetThreeUrnString);
  protected static RelatedEntity downstreamOfDatasetFourRelatedEntity = new RelatedEntity(downstreamOf, datasetFourUrnString);
  protected static RelatedEntity hasOwnerDatasetOneRelatedEntity = new RelatedEntity(hasOwner, datasetOneUrnString);
  protected static RelatedEntity hasOwnerDatasetTwoRelatedEntity = new RelatedEntity(hasOwner, datasetTwoUrnString);
  protected static RelatedEntity hasOwnerDatasetThreeRelatedEntity = new RelatedEntity(hasOwner, datasetThreeUrnString);
  protected static RelatedEntity hasOwnerDatasetFourRelatedEntity = new RelatedEntity(hasOwner, datasetFourUrnString);
  protected static RelatedEntity hasOwnerUserOneRelatedEntity = new RelatedEntity(hasOwner, userOneUrnString);
  protected static RelatedEntity hasOwnerUserTwoRelatedEntity = new RelatedEntity(hasOwner, userTwoUrnString);
  protected static RelatedEntity knowsUserOneRelatedEntity = new RelatedEntity(knowsUser, userOneUrnString);
  protected static RelatedEntity knowsUserTwoRelatedEntity = new RelatedEntity(knowsUser, userTwoUrnString);

  /**
   * Some relationship filters.
   */
  protected static RelationshipFilter outgoingRelationships = createRelationshipFilter(RelationshipDirection.OUTGOING);
  protected static RelationshipFilter incomingRelationships = createRelationshipFilter(RelationshipDirection.INCOMING);
  protected static RelationshipFilter undirectedRelationships = createRelationshipFilter(RelationshipDirection.UNDIRECTED);

  /**
   * Any source and destination type value.
   */
  protected static @Nullable String anyType = null;

  @Test
  public void testStaticUrns() {
    assertNotNull(datasetOneUrn);
    assertNotNull(datasetTwoUrn);
    assertNotNull(datasetThreeUrn);
    assertNotNull(datasetFourUrn);

    assertNotNull(userOneUrn);
    assertNotNull(userTwoUrn);
  }

  /**
   * Provides the current GraphService instance to test. This is being called by the test method
   * at most once. The serviced graph should be empty.
   *
   * @return the GraphService instance to test
   * @throws Exception on failure
   */
  @Nonnull
  abstract protected GraphService getGraphService() throws Exception;

  /**
   * Allows the specific GraphService test implementation to wait for GraphService writes to
   * be synced / become available to reads.
   *
   * @throws Exception on failure
   */
  abstract protected void syncAfterWrite() throws Exception;

  /**
   * Calls getGraphService to retrieve the test GraphService and populates it
   * with edges via `GraphService.addEdge`.
   *
   * @return test GraphService
   * @throws Exception on failure
   */
  protected GraphService getPopulatedGraphService() throws Exception {
    GraphService service = getGraphService();

    List<Edge> edges = Arrays.asList(
            new Edge(datasetTwoUrn, datasetOneUrn, downstreamOf),
            new Edge(datasetThreeUrn, datasetTwoUrn, downstreamOf),
            new Edge(datasetFourUrn, datasetTwoUrn, downstreamOf),

            new Edge(datasetOneUrn, userOneUrn, hasOwner),
            new Edge(datasetTwoUrn, userOneUrn, hasOwner),
            new Edge(datasetThreeUrn, userTwoUrn, hasOwner),
            new Edge(datasetFourUrn, userTwoUrn, hasOwner),

            new Edge(userOneUrn, userTwoUrn, knowsUser),
            new Edge(userTwoUrn, userOneUrn, knowsUser)
    );

    edges.forEach(service::addEdge);
    syncAfterWrite();

    return service;
  }

  protected static RelationshipFilter createRelationshipFilter(RelationshipDirection direction) {
    return createRelationshipFilter(EMPTY_FILTER, direction);
  }

  protected static RelationshipFilter createRelationshipFilter(@Nonnull Filter filter,
                                                               @Nonnull RelationshipDirection direction) {
    return new RelationshipFilter()
            .setCriteria(filter.getCriteria())
            .setDirection(direction);
  }

  protected static @Nullable
  Urn createFromString(@Nonnull String rawUrn) {
    try {
      return Urn.createFromString(rawUrn);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  protected static void assertEqualsAnyOrder(RelatedEntitiesResult actual, List<RelatedEntity> expected) {
      assertEqualsAnyOrder(actual, new RelatedEntitiesResult(0, expected.size(), expected.size(), expected));
  }

  protected static void assertEqualsAnyOrder(RelatedEntitiesResult actual, RelatedEntitiesResult expected) {
    // assertEquals(actual.start, expected.start);
    //assertEquals(actual.count, expected.count);
    //assertEquals(actual.total, expected.total);
    assertEqualsAnyOrder(actual.entities, expected.entities);
  }

  protected static <T> void assertEqualsAnyOrder(List<T> actual, List<T> expected) {
    // TODO: expect no duplicates
    assertEquals(
            new HashSet<>(actual),
            new HashSet<>(expected)
    );
  }

  @DataProvider(name = "FindRelatedEntitiesSourceEntityFilterTests")
  public Object[][] getFindRelatedEntitiesSourceEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },

            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity)
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity)
            }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesSourceEntityFilterTests")
  public void testFindRelatedEntitiesSourceEntityFilter(Filter sourceEntityFilter,
                                                        List<String> relationshipTypes,
                                                        RelationshipFilter relationships,
                                                        List<RelatedEntity> expectedRelatedEntities) throws Exception {
    doTestFindRelatedEntities(
            sourceEntityFilter,
            EMPTY_FILTER,
            relationshipTypes,
            relationships,
            expectedRelatedEntities
    );
  }

  @DataProvider(name = "FindRelatedEntitiesDestinationEntityFilterTests")
  public Object[][] getFindRelatedEntitiesDestinationEntityFilterTests() {
    return new Object[][] {
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", datasetTwoUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity)
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList()
            },

            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity)
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    newFilter("urn", userOneUrnString),
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity)
            }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesDestinationEntityFilterTests")
  public void testFindRelatedEntitiesDestinationEntityFilter(Filter destinationEntityFilter,
                                                             List<String> relationshipTypes,
                                                             RelationshipFilter relationships,
                                                             List<RelatedEntity> expectedRelatedEntities) throws Exception {
    doTestFindRelatedEntities(
            EMPTY_FILTER,
            destinationEntityFilter,
            relationshipTypes,
            relationships,
            expectedRelatedEntities
    );
  }

  private void doTestFindRelatedEntities(
          final Filter sourceEntityFilter,
          final Filter destinationEntityFilter,
          List<String> relationshipTypes,
          final RelationshipFilter relationshipFilter,
          List<RelatedEntity> expectedRelatedEntities
  ) throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities = service.findRelatedEntities(
            anyType, sourceEntityFilter,
            anyType, destinationEntityFilter,
            relationshipTypes, relationshipFilter,
            0, 10
    );

    assertEqualsAnyOrder(relatedEntities, expectedRelatedEntities);
  }

  @DataProvider(name = "FindRelatedEntitiesSourceTypeTests")
  public Object[][] getFindRelatedEntitiesSourceTypeTests() {
    return new Object[][]{
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },

            // "" used to be any type before v0.9.0, which is now encoded by null
            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Collections.emptyList()
            },
            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Collections.emptyList()
            },
            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Collections.emptyList()
            },

            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[]{
                    datasetType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },

            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList()
            },

            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
            },
            new Object[]{
                    userType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
            }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesSourceTypeTests")
  public void testFindRelatedEntitiesSourceType(String datasetType,
                                                List<String> relationshipTypes,
                                                RelationshipFilter relationships,
                                                List<RelatedEntity> expectedRelatedEntities) throws Exception {
    doTestFindRelatedEntities(
            datasetType,
            anyType,
            relationshipTypes,
            relationships,
            expectedRelatedEntities
    );
  }

  @DataProvider(name = "FindRelatedEntitiesDestinationTypeTests")
  public Object[][] getFindRelatedEntitiesDestinationTypeTests() {
    return new Object[][] {
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[] {
                    null,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },

            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Collections.emptyList()
            },
            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Collections.emptyList()
            },
            new Object[] {
                    "",
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Collections.emptyList()
            },

            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },

            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
            },
            new Object[] {
                    datasetType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
            },

            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    outgoingRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
            },
            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    incomingRelationships,
                    Arrays.asList()
            },
            new Object[] {
                    userType,
                    Arrays.asList(hasOwner),
                    undirectedRelationships,
                    Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
            }
    };
  }

  @Test(dataProvider = "FindRelatedEntitiesDestinationTypeTests")
  public void testFindRelatedEntitiesDestinationType(String datasetType,
                                                     List<String> relationshipTypes,
                                                     RelationshipFilter relationships,
                                                     List<RelatedEntity> expectedRelatedEntities) throws Exception {
    doTestFindRelatedEntities(
            anyType,
            datasetType,
            relationshipTypes,
            relationships,
            expectedRelatedEntities
    );
  }

  private void doTestFindRelatedEntities(
          final String sourceType,
          final String destinationType,
          final List<String> relationshipTypes,
          final RelationshipFilter relationshipFilter,
          List<RelatedEntity> expectedRelatedEntities
  ) throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities = service.findRelatedEntities(
            sourceType, EMPTY_FILTER,
            destinationType, EMPTY_FILTER,
            relationshipTypes, relationshipFilter,
            0, 10
    );

    assertEqualsAnyOrder(relatedEntities, expectedRelatedEntities);
  }

  @Test
  public void testFindRelatedEntitiesNoRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities = service.findRelatedEntities(
            anyType, EMPTY_FILTER,
            anyType, EMPTY_FILTER,
            Collections.emptyList(), outgoingRelationships,
            0, 10
    );

    assertEquals(relatedEntities.entities, Collections.emptyList());

    // does the test actually test something? is the Collections.emptyList() the only reason why we did not get any related urns?
    RelatedEntitiesResult relatedEntitiesAll = service.findRelatedEntities(
            anyType, EMPTY_FILTER,
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser), outgoingRelationships,
            0, 10
    );

    assertNotEquals(relatedEntities, Collections.emptyList());
  }

  @Test
  public void testFindRelatedEntitiesAllFilters() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedEntities = service.findRelatedEntities(
            datasetType, newFilter("urn", datasetOneUrnString),
            userType, newFilter("urn", userOneUrnString),
            Arrays.asList(hasOwner), outgoingRelationships,
            0, 10
    );

    assertEquals(relatedEntities.entities, Arrays.asList(hasOwnerUserOneRelatedEntity));

    relatedEntities = service.findRelatedEntities(
            datasetType, newFilter("urn", datasetOneUrnString),
            userType, newFilter("urn", userTwoUrnString),
            Arrays.asList(hasOwner), incomingRelationships,
            0, 10
    );

    assertEquals(relatedEntities.entities, Collections.emptyList());
  }

  @Test
  public void testFindRelatedEntitiesOffsetAndCount() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult allRelatedEntities = service.findRelatedEntities(
            datasetType, EMPTY_FILTER,
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf), outgoingRelationships,
            0, 100
    );

    assertEqualsAnyOrder(allRelatedEntities, Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity));

    List<RelatedEntity> individualRelatedEntities = new ArrayList<>();
    IntStream.range(0, allRelatedEntities.entities.size())
            .forEach(idx -> individualRelatedEntities.addAll(
                    service.findRelatedEntities(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), outgoingRelationships,
                    idx, 1
                ).entities
            ));
    Assert.assertEquals(individualRelatedEntities, allRelatedEntities.entities);
  }

  @DataProvider(name = "RemoveEdgesFromNodeTests")
  public Object[][] getRemoveEdgesFromNodeTests() {
    return new Object[][] {
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    outgoingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity),
                    Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
                    Arrays.asList(),
                    Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
            },
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    incomingRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity),
                    Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity),
                    Arrays.asList(),
            },
            new Object[] {
                    datasetTwoUrn,
                    Arrays.asList(downstreamOf),
                    undirectedRelationships,
                    Arrays.asList(downstreamOfDatasetOneRelatedEntity),
                    Arrays.asList(downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity),
                    Arrays.asList(),
                    Arrays.asList()
            },

            new Object[] {
                    userOneUrn,
                    Arrays.asList(hasOwner, knowsUser),
                    outgoingRelationships,
                    Arrays.asList(knowsUserTwoRelatedEntity),
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, knowsUserOneRelatedEntity),
                    Arrays.asList(),
                    Arrays.asList(datasetOneUrnString, datasetTwoUrnString, userTwoUrnString)
            },
            new Object[] {
                    userOneUrn,
                    Arrays.asList(hasOwner, knowsUser),
                    incomingRelationships,
                    Arrays.asList(knowsUserTwoRelatedEntity),
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, knowsUserTwoRelatedEntity),
                    Arrays.asList(knowsUserTwoRelatedEntity),
                    Arrays.asList()
            },
            new Object[] {
                    userOneUrn,
                    Arrays.asList(hasOwner, knowsUser),
                    undirectedRelationships,
                    Arrays.asList(knowsUserTwoRelatedEntity),
                    Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity, knowsUserTwoRelatedEntity),
                    Arrays.asList(),
                    Arrays.asList()
            }
    };
  }

  @Test(dataProvider = "RemoveEdgesFromNodeTests")
  public void testRemoveEdgesFromNode(@Nonnull Urn nodeToRemoveFrom,
                                      @Nonnull List<String> relationTypes,
                                      @Nonnull RelationshipFilter relationshipFilter,
                                      List<RelatedEntity> expectedOutgoingRelatedUrnsBeforeRemove,
                                      List<RelatedEntity> expectedIncomingRelatedUrnsBeforeRemove,
                                      List<RelatedEntity> expectedOutgoingRelatedUrnsAfterRemove,
                                      List<RelatedEntity> expectedIncomingRelatedUrnsAfterRemove) throws Exception {
    GraphService service = getPopulatedGraphService();

    List<String> allOtherRelationTypes =
            allRelationshipTypes.stream()
                    .filter(relation -> !relationTypes.contains(relation))
                    .collect(Collectors.toList());
    assertTrue(allOtherRelationTypes.size() > 0);

    RelatedEntitiesResult actualOutgoingRelatedUrnsBeforeRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, outgoingRelationships,
            0, 10);
    RelatedEntitiesResult actualIncomingRelatedUrnsBeforeRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, incomingRelationships,
            0, 10);
    assertEqualsAnyOrder(actualOutgoingRelatedUrnsBeforeRemove, expectedOutgoingRelatedUrnsBeforeRemove);
    assertEqualsAnyOrder(actualIncomingRelatedUrnsBeforeRemove, expectedIncomingRelatedUrnsBeforeRemove);

    // we expect these do not change
    RelatedEntitiesResult relatedEntitiesOfOtherRelationTypesBeforeRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            allOtherRelationTypes, undirectedRelationships,
            0, 10);

    service.removeEdgesFromNode(
            nodeToRemoveFrom,
            relationTypes,
            relationshipFilter
    );
    syncAfterWrite();

    RelatedEntitiesResult actualOutgoingRelatedUrnsAfterRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, outgoingRelationships,
            0, 10);
    RelatedEntitiesResult actualIncomingRelatedUrnsAfterRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            relationTypes, incomingRelationships,
            0, 10);
    assertEqualsAnyOrder(actualOutgoingRelatedUrnsAfterRemove, expectedOutgoingRelatedUrnsAfterRemove);
    assertEqualsAnyOrder(actualIncomingRelatedUrnsAfterRemove, expectedIncomingRelatedUrnsAfterRemove);

    // assert these did not change
    RelatedEntitiesResult relatedEntitiesOfOtherRelationTypesAfterRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            allOtherRelationTypes, undirectedRelationships,
            0, 10);
    assertEqualsAnyOrder(relatedEntitiesOfOtherRelationTypesAfterRemove, relatedEntitiesOfOtherRelationTypesBeforeRemove);
  }

  @Test
  public void testRemoveEdgesFromNodeNoRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();
    Urn nodeToRemoveFrom = datasetOneUrn;

    RelatedEntitiesResult relatedEntitiesBeforeRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser), undirectedRelationships,
            0, 10);

    service.removeEdgesFromNode(
            nodeToRemoveFrom,
            Collections.emptyList(),
            undirectedRelationships
    );
    syncAfterWrite();

    RelatedEntitiesResult relatedEntitiesAfterRemove = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser), undirectedRelationships,
            0, 10);
    assertEqualsAnyOrder(relatedEntitiesBeforeRemove, relatedEntitiesAfterRemove);

    // does the test actually test something? is the Collections.emptyList() the only reason why we did not see changes?
    service.removeEdgesFromNode(
            nodeToRemoveFrom,
            Arrays.asList(downstreamOf, hasOwner, knowsUser),
            undirectedRelationships
    );
    syncAfterWrite();

    RelatedEntitiesResult relatedEntitiesAfterRemoveAll = service.findRelatedEntities(
            anyType, newFilter("urn", nodeToRemoveFrom.toString()),
            anyType, EMPTY_FILTER,
            Arrays.asList(downstreamOf, hasOwner, knowsUser), undirectedRelationships,
            0, 10);
    assertEqualsAnyOrder(relatedEntitiesAfterRemoveAll, Collections.emptyList());
  }

  @Test
  public void testRemoveNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    // assert the initial graph: check all nodes related to DownstreamOf and hasOwner edges
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
    );

    service.removeNode(datasetTwoUrn);
    syncAfterWrite();

    // assert the modified graph: check all nodes related to DownstreamOf and hasOwner edges
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
    );
  }

  @Test
  public void testClear() throws Exception {
    GraphService service = getPopulatedGraphService();

    // assert the initial graph: check all nodes related to upstreamOf and nextVersionOf edges
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    anyType, EMPTY_FILTER,
                    datasetType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity, downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    anyType, EMPTY_FILTER,
                    userType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    anyType, EMPTY_FILTER,
                    userType, EMPTY_FILTER,
                    Arrays.asList(knowsUser), undirectedRelationships,
                    0, 10
            ),
            Arrays.asList(knowsUserOneRelatedEntity, knowsUserTwoRelatedEntity)
    );

    service.clear();
    syncAfterWrite();

    // assert the modified graph: check all nodes related to upstreamOf and nextVersionOf edges again
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    datasetType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(downstreamOf), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    userType, EMPTY_FILTER,
                    anyType, EMPTY_FILTER,
                    Arrays.asList(hasOwner), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
    assertEqualsAnyOrder(
            service.findRelatedEntities(
                    anyType, EMPTY_FILTER,
                    userType, EMPTY_FILTER,
                    Arrays.asList(knowsUser), undirectedRelationships,
                    0, 10
            ),
            Collections.emptyList()
    );
  }

}
