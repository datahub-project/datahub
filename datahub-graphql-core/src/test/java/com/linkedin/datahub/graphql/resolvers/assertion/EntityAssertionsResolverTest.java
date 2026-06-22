package com.linkedin.datahub.graphql.resolvers.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityAssertionsResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityAssertionsResolverTest {
  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn assertionUrn = Urn.createFromString("urn:li:assertion:test-guid");

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(datasetUrn.toString()),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(10),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship().setEntity(assertionUrn).setType("Asserts")))));

    Map<String, com.linkedin.entity.EnvelopedAspect> assertionAspects = new HashMap<>();
    assertionAspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId("test-guid").data())));
    assertionAspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                                .setAggregation(AssertionStdAggregation.MAX)
                                .setOperator(AssertionStdOperator.EQUAL_TO)
                                .setFields(
                                    new UrnArray(
                                        ImmutableList.of(
                                            Urn.createFromString(
                                                "urn:li:schemaField:(urn:li:dataset:(test,test,test),fieldPath)"))))
                                .setParameters(
                                    new AssertionStdParameters()
                                        .setValue(
                                            new AssertionStdParameter()
                                                .setValue("10")
                                                .setType(AssertionStdParameterType.NUMBER))))
                        .data())));
    assertionAspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new DataPlatformInstance()
                        .setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"))
                        .data())));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(assertionUrn)),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableMap.of(
                assertionUrn,
                new EntityResponse()
                    .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                    .setUrn(assertionUrn)
                    .setAspects(new EnvelopedAspectMap(assertionAspects))));

    EntityAssertionsResolver resolver = new EntityAssertionsResolver(mockClient, graphClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.eq(200))).thenReturn(10);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("includeSoftDeleted"), Mockito.eq(false)))
        .thenReturn(false);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentEntity = new Dataset();
    parentEntity.setUrn(datasetUrn.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentEntity);

    EntityAssertionsResult result = resolver.get(mockEnv).get();

    Mockito.verify(graphClient, Mockito.times(1))
        .getRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    Mockito.verify(mockClient, Mockito.times(1))
        .batchGetV2(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

    // Regression guard: the per-URN exists() call was the N+1 source. The resolver must now read
    // soft-delete state from the already-loaded status aspect instead.
    Mockito.verify(mockClient, Mockito.never())
        .exists(Mockito.any(), Mockito.any(Urn.class), Mockito.any());

    // Assert that GraphQL assertion run event matches expectations
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);

    com.linkedin.datahub.graphql.generated.Assertion assertion =
        resolver.get(mockEnv).get().getAssertions().get(0);
    assertEquals(assertion.getUrn(), assertionUrn.toString());
    assertEquals(assertion.getType(), EntityType.ASSERTION);
    assertEquals(assertion.getPlatform().getUrn(), "urn:li:dataPlatform:hive");
    assertEquals(
        assertion.getInfo().getType(),
        com.linkedin.datahub.graphql.generated.AssertionType.DATASET);
    assertEquals(assertion.getInfo().getDatasetAssertion().getDatasetUrn(), datasetUrn.toString());
    assertEquals(
        assertion.getInfo().getDatasetAssertion().getScope(),
        com.linkedin.datahub.graphql.generated.DatasetAssertionScope.DATASET_COLUMN);
    assertEquals(
        assertion.getInfo().getDatasetAssertion().getAggregation(),
        com.linkedin.datahub.graphql.generated.AssertionStdAggregation.MAX);
    assertEquals(
        assertion.getInfo().getDatasetAssertion().getOperator(),
        com.linkedin.datahub.graphql.generated.AssertionStdOperator.EQUAL_TO);
    assertEquals(
        assertion.getInfo().getDatasetAssertion().getParameters().getValue().getType(),
        com.linkedin.datahub.graphql.generated.AssertionStdParameterType.NUMBER);
    assertEquals(
        assertion.getInfo().getDatasetAssertion().getParameters().getValue().getValue(), "10");
  }

  /**
   * Two assertions are returned by the graph store. One has {@code status.removed = true}. With
   * {@code includeSoftDeleted=false} (the default), it must be filtered out and only the
   * non-removed assertion should appear in the result. Critically, no per-URN {@code exists} call
   * is made — the soft-delete state is read from the {@code status} aspect already returned by the
   * single {@code batchGetV2} call.
   */
  @Test
  public void testSoftDeletedAssertionFilteredOut() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn liveAssertionUrn = Urn.createFromString("urn:li:assertion:live");
    Urn removedAssertionUrn = Urn.createFromString("urn:li:assertion:removed");

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(datasetUrn.toString()),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(2)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship().setEntity(liveAssertionUrn).setType("Asserts"),
                            new EntityRelationship()
                                .setEntity(removedAssertionUrn)
                                .setType("Asserts")))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(liveAssertionUrn, removedAssertionUrn)),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableMap.of(
                liveAssertionUrn,
                assertionResponse(liveAssertionUrn, datasetUrn, "live-guid", false),
                removedAssertionUrn,
                assertionResponse(removedAssertionUrn, datasetUrn, "removed-guid", true)));

    EntityAssertionsResolver resolver = new EntityAssertionsResolver(mockClient, graphClient);

    EntityAssertionsResult result =
        resolver.get(mockEnv(datasetUrn, false /* includeSoftDeleted */)).get();

    // The removed assertion must be excluded.
    assertEquals(result.getAssertions().size(), 1);
    assertEquals(result.getAssertions().get(0).getUrn(), liveAssertionUrn.toString());

    // The N+1 source must remain gone.
    Mockito.verify(mockClient, Mockito.never())
        .exists(Mockito.any(), Mockito.any(Urn.class), Mockito.any());
  }

  /**
   * When {@code includeSoftDeleted=true}, soft-deleted assertions must be retained alongside live
   * ones, matching the legacy semantics of {@code exists(urn, true)}.
   */
  @Test
  public void testIncludeSoftDeletedReturnsRemovedAssertions() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn liveUrn = Urn.createFromString("urn:li:assertion:live");
    Urn removedUrn = Urn.createFromString("urn:li:assertion:removed");

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(datasetUrn.toString()),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(2)
                .setTotal(2)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship().setEntity(liveUrn).setType("Asserts"),
                            new EntityRelationship().setEntity(removedUrn).setType("Asserts")))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(liveUrn, removedUrn)),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableMap.of(
                liveUrn,
                assertionResponse(liveUrn, datasetUrn, "live-guid", false),
                removedUrn,
                assertionResponse(removedUrn, datasetUrn, "removed-guid", true)));

    EntityAssertionsResolver resolver = new EntityAssertionsResolver(mockClient, graphClient);

    EntityAssertionsResult result =
        resolver.get(mockEnv(datasetUrn, true /* includeSoftDeleted */)).get();

    assertEquals(result.getAssertions().size(), 2);
    Mockito.verify(mockClient, Mockito.never())
        .exists(Mockito.any(), Mockito.any(Urn.class), Mockito.any());
  }

  /**
   * When the {@code status} aspect is entirely absent (e.g. older entities ingested before the
   * status aspect existed), the assertion must default to being included — matching the legacy
   * {@code EntityServiceImpl.exists(urn, false)} behavior, which treats "no status aspect" as "not
   * removed".
   */
  @Test
  public void testAssertionWithoutStatusAspectDefaultsToIncluded() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    GraphClient graphClient = Mockito.mock(GraphClient.class);

    Urn datasetUrn = Urn.createFromString("urn:li:dataset:(test,test,test)");
    Urn urn = Urn.createFromString("urn:li:assertion:no-status");

    Mockito.when(
            graphClient.getRelatedEntities(
                Mockito.eq(datasetUrn.toString()),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setStart(0)
                .setCount(1)
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship().setEntity(urn).setType("Asserts")))));

    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(urn)),
                Mockito.eq(null)))
        .thenReturn(ImmutableMap.of(urn, buildResponseNoStatus(urn, datasetUrn, "guid")));

    EntityAssertionsResolver resolver = new EntityAssertionsResolver(mockClient, graphClient);

    EntityAssertionsResult result =
        resolver.get(mockEnv(datasetUrn, false /* includeSoftDeleted */)).get();

    assertEquals(result.getAssertions().size(), 1);
    Mockito.verify(mockClient, Mockito.never())
        .exists(Mockito.any(), Mockito.any(Urn.class), Mockito.any());
  }

  // ---------- Helpers ----------

  private static DataFetchingEnvironment mockEnv(Urn datasetUrn, boolean includeSoftDeleted) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getArgumentOrDefault(Mockito.eq("start"), Mockito.eq(0))).thenReturn(0);
    Mockito.when(env.getArgumentOrDefault(Mockito.eq("count"), Mockito.eq(200))).thenReturn(10);
    Mockito.when(env.getArgumentOrDefault(Mockito.eq("includeSoftDeleted"), Mockito.eq(false)))
        .thenReturn(includeSoftDeleted);
    Mockito.when(env.getContext()).thenReturn(mockContext);
    Dataset parent = new Dataset();
    parent.setUrn(datasetUrn.toString());
    Mockito.when(env.getSource()).thenReturn(parent);
    return env;
  }

  private static EntityResponse assertionResponse(
      Urn assertionUrn, Urn datasetUrn, String guid, boolean removed) throws Exception {
    Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId(guid).data())));
    aspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                                .setAggregation(AssertionStdAggregation.MAX)
                                .setOperator(AssertionStdOperator.EQUAL_TO))
                        .data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new com.linkedin.common.Status().setRemoved(removed).data())));
    return new EntityResponse()
        .setEntityName(Constants.ASSERTION_ENTITY_NAME)
        .setUrn(assertionUrn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  private static EntityResponse buildResponseNoStatus(Urn assertionUrn, Urn datasetUrn, String guid)
      throws Exception {
    Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AssertionKey().setAssertionId(guid).data())));
    aspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new Aspect(
                    new AssertionInfo()
                        .setType(AssertionType.DATASET)
                        .setDatasetAssertion(
                            new DatasetAssertionInfo()
                                .setDataset(datasetUrn)
                                .setScope(DatasetAssertionScope.DATASET_COLUMN)
                                .setAggregation(AssertionStdAggregation.MAX)
                                .setOperator(AssertionStdOperator.EQUAL_TO))
                        .data())));
    // Intentionally no status aspect.
    return new EntityResponse()
        .setEntityName(Constants.ASSERTION_ENTITY_NAME)
        .setUrn(assertionUrn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
