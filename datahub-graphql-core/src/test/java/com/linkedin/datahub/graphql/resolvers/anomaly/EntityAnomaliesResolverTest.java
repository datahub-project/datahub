package com.linkedin.datahub.graphql.resolvers.anomaly;

import static com.linkedin.datahub.graphql.resolvers.anomaly.EntityAnomaliesResolver.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.anomaly.AnomalyInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityAnomaliesResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.AnomalyKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class EntityAnomaliesResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(test,test,test)");
  private static final Urn TEST_ANOMALY_URN = UrnUtils.getUrn("urn:li:anomaly:test-guid");

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Map<String, com.linkedin.entity.EnvelopedAspect> anomalyAspects = new HashMap<>();
    anomalyAspects.put(
        Constants.ANOMALY_KEY_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new Aspect(new AnomalyKey().setId("test-guid").data())));

    AnomalyInfo expectedInfo =
        new AnomalyInfo()
            .setType(com.linkedin.anomaly.AnomalyType.DATASET_COLUMN)
            .setEntity(TEST_DATASET_URN)
            .setStatus(
                new com.linkedin.anomaly.AnomalyStatus()
                    .setState(com.linkedin.anomaly.AnomalyState.ACTIVE)
                    .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
            .setReview(
                new com.linkedin.anomaly.AnomalyReview()
                    .setState(com.linkedin.anomaly.AnomalyReviewState.PENDING)
                    .setLastUpdated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN)))
            .setSource(
                new com.linkedin.anomaly.AnomalySource()
                    .setType(com.linkedin.anomaly.AnomalySourceType.INFERRED_ASSERTION_FAILURE))
            .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN));

    anomalyAspects.put(
        Constants.ANOMALY_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(expectedInfo.data())));

    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(ANOMALY_ENTITIES_SEARCH_INDEX_FIELD_NAME, TEST_DATASET_URN.toString());
    Filter expectedFilter = QueryUtils.newFilter(criterionMap);

    SortCriterion expectedSort = new SortCriterion();
    expectedSort.setField(CREATED_TIME_SEARCH_INDEX_FIELD_NAME);
    expectedSort.setOrder(SortOrder.DESCENDING);

    Mockito.when(
            mockClient.filter(
                Mockito.any(),
                Mockito.eq(Constants.ANOMALY_ENTITY_NAME),
                Mockito.eq(expectedFilter),
                Mockito.eq(Collections.singletonList(expectedSort)),
                Mockito.eq(0),
                Mockito.eq(10)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(1)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableSet.of(new SearchEntity().setEntity(TEST_ANOMALY_URN)))));

    EntityAnomaliesResolver resolver = new EntityAnomaliesResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("start"), Mockito.eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(Mockito.eq("count"), Mockito.eq(20))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Dataset parentEntity = new Dataset();
    parentEntity.setUrn(TEST_DATASET_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentEntity);

    EntityAnomaliesResult result = resolver.get(mockEnv).get();

    // Assert that GraphQL Anomaly run event matches expectations
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getTotal(), 1);

    com.linkedin.datahub.graphql.generated.Anomaly anomaly =
        resolver.get(mockEnv).get().getAnomalies().get(0);
    assertEquals(anomaly.getUrn(), TEST_ANOMALY_URN.toString());
    assertEquals(anomaly.getType(), EntityType.ANOMALY);
  }
}
