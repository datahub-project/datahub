package com.linkedin.datahub.graphql.types.view;

import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataHubViewTypeTest {

  private static final String TEST_VIEW_URN = "urn:li:dataHubView:test";
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  /**
   * A Valid View is one which is minted by the createView or updateView GraphQL resolvers.
   *
   * <p>View Definitions currently support a limited Filter structure, which includes a single
   * Logical filter set. Either a set of OR criteria with 1 value in each nested "and", or a single
   * OR criteria with a set of nested ANDs.
   *
   * <p>This enables us to easily support merging more complex View predicates in the future without
   * a data migration, should the need arise.
   */
  private static final DataHubViewInfo TEST_VALID_VIEW_INFO =
      new DataHubViewInfo()
          .setType(DataHubViewType.PERSONAL)
          .setName("test")
          .setDescription("test description")
          .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
          .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
          .setDefinition(
              new DataHubViewDefinition()
                  .setFilter(
                      new Filter()
                          .setOr(
                              new ConjunctiveCriterionArray(
                                  ImmutableList.of(
                                      new ConjunctiveCriterion()
                                          .setAnd(
                                              new CriterionArray(
                                                  ImmutableList.of(
                                                      new Criterion()
                                                          .setValues(
                                                              new StringArray(
                                                                  ImmutableList.of(
                                                                      "value1", "value2")))
                                                          .setField("test")
                                                          .setCondition(Condition.EQUAL))))))))
                  .setEntityTypes(
                      new StringArray(
                          ImmutableList.of(
                              Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME))));

  /**
   * An Invalid View is on which has been ingested manually, which should not occur under normal
   * operation of DataHub.
   *
   * <p>This would be a complex view with multiple OR and nested AND predicates.
   */
  private static final DataHubViewInfo TEST_INVALID_VIEW_INFO =
      new DataHubViewInfo()
          .setType(DataHubViewType.PERSONAL)
          .setName("test")
          .setDescription("test description")
          .setCreated(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
          .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_USER_URN))
          .setDefinition(
              new DataHubViewDefinition()
                  .setFilter(
                      new Filter()
                          .setOr(
                              new ConjunctiveCriterionArray(
                                  ImmutableList.of(
                                      new ConjunctiveCriterion()
                                          .setAnd(
                                              new CriterionArray(
                                                  ImmutableList.of(
                                                      new Criterion()
                                                          .setValues(
                                                              new StringArray(
                                                                  ImmutableList.of(
                                                                      "value1", "value2")))
                                                          .setField("test")
                                                          .setCondition(Condition.EQUAL),
                                                      new Criterion()
                                                          .setValues(
                                                              new StringArray(
                                                                  ImmutableList.of(
                                                                      "value1", "value2")))
                                                          .setField("test2")
                                                          .setCondition(Condition.EQUAL)))),
                                      new ConjunctiveCriterion()
                                          .setAnd(
                                              new CriterionArray(
                                                  ImmutableList.of(
                                                      new Criterion()
                                                          .setValues(
                                                              new StringArray(
                                                                  ImmutableList.of(
                                                                      "value1", "value2")))
                                                          .setField("test2")
                                                          .setCondition(Condition.EQUAL),
                                                      new Criterion()
                                                          .setValues(
                                                              new StringArray(
                                                                  ImmutableList.of(
                                                                      "value1", "value2")))
                                                          .setField("test2")
                                                          .setCondition(Condition.EQUAL))))))))
                  .setEntityTypes(
                      new StringArray(
                          ImmutableList.of(
                              Constants.DATASET_ENTITY_NAME, Constants.DASHBOARD_ENTITY_NAME))));

  private static final String TEST_VIEW_URN_2 = "urn:li:dataHubView:test2";

  @Test
  public void testBatchLoadValidView() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn viewUrn1 = Urn.createFromString(TEST_VIEW_URN);
    Urn viewUrn2 = Urn.createFromString(TEST_VIEW_URN_2);

    Map<String, EnvelopedAspect> view1Aspects = new HashMap<>();
    view1Aspects.put(
        Constants.DATAHUB_VIEW_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_VALID_VIEW_INFO.data())));
    Mockito.when(
            client.batchGetV2(
                Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(viewUrn1, viewUrn2))),
                Mockito.eq(
                    com.linkedin.datahub.graphql.types.view.DataHubViewType.ASPECTS_TO_FETCH),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                viewUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DATAHUB_VIEW_ENTITY_NAME)
                    .setUrn(viewUrn1)
                    .setAspects(new EnvelopedAspectMap(view1Aspects))));

    com.linkedin.datahub.graphql.types.view.DataHubViewType type =
        new com.linkedin.datahub.graphql.types.view.DataHubViewType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<DataHubView>> result =
        type.batchLoad(ImmutableList.of(TEST_VIEW_URN, TEST_VIEW_URN_2), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(viewUrn1, viewUrn2)),
            Mockito.eq(com.linkedin.datahub.graphql.types.view.DataHubViewType.ASPECTS_TO_FETCH),
            Mockito.any(Authentication.class));

    assertEquals(result.size(), 2);

    DataHubView view = result.get(0).getData();
    assertEquals(view.getUrn(), TEST_VIEW_URN);
    assertEquals(view.getType(), EntityType.DATAHUB_VIEW);
    assertEquals(view.getViewType().toString(), DataHubViewType.PERSONAL.toString());
    assertEquals(view.getName(), TEST_VALID_VIEW_INFO.getName());
    assertEquals(view.getDescription(), TEST_VALID_VIEW_INFO.getDescription());
    assertEquals(view.getDefinition().getEntityTypes().size(), 2);
    assertEquals(view.getDefinition().getEntityTypes().get(0), EntityType.DATASET);
    assertEquals(view.getDefinition().getEntityTypes().get(1), EntityType.DASHBOARD);
    assertEquals(view.getDefinition().getFilter().getOperator(), LogicalOperator.AND);
    assertEquals(view.getDefinition().getFilter().getFilters().size(), 1);
    assertEquals(
        view.getDefinition().getFilter().getFilters().get(0).getCondition(), FilterOperator.EQUAL);
    assertEquals(view.getDefinition().getFilter().getFilters().get(0).getField(), "test");
    assertEquals(
        view.getDefinition().getFilter().getFilters().get(0).getValues(),
        ImmutableList.of("value1", "value2"));

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadInvalidView() throws Exception {
    // If an Invalid View Definition is found in MySQL, we will return an Empty no-op View. (and log
    // a warning).
    EntityClient client = Mockito.mock(EntityClient.class);
    Urn invalidViewUrn = Urn.createFromString(TEST_VIEW_URN);

    Map<String, EnvelopedAspect> view1Aspects = new HashMap<>();
    view1Aspects.put(
        Constants.DATAHUB_VIEW_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_INVALID_VIEW_INFO.data())));
    Mockito.when(
            client.batchGetV2(
                Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(invalidViewUrn))),
                Mockito.eq(
                    com.linkedin.datahub.graphql.types.view.DataHubViewType.ASPECTS_TO_FETCH),
                Mockito.any(Authentication.class)))
        .thenReturn(
            ImmutableMap.of(
                invalidViewUrn,
                new EntityResponse()
                    .setEntityName(Constants.DATAHUB_VIEW_ENTITY_NAME)
                    .setUrn(invalidViewUrn)
                    .setAspects(new EnvelopedAspectMap(view1Aspects))));

    com.linkedin.datahub.graphql.types.view.DataHubViewType type =
        new com.linkedin.datahub.graphql.types.view.DataHubViewType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<DataHubView>> result =
        type.batchLoad(ImmutableList.of(TEST_VIEW_URN), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            Mockito.eq(Constants.DATAHUB_VIEW_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(invalidViewUrn)),
            Mockito.eq(com.linkedin.datahub.graphql.types.view.DataHubViewType.ASPECTS_TO_FETCH),
            Mockito.any(Authentication.class));

    assertEquals(result.size(), 1);

    DataHubView view = result.get(0).getData();
    assertEquals(view.getUrn(), TEST_VIEW_URN);
    assertEquals(view.getType(), EntityType.DATAHUB_VIEW);
    assertEquals(view.getViewType().toString(), DataHubViewType.PERSONAL.toString());
    assertEquals(view.getName(), TEST_INVALID_VIEW_INFO.getName());
    assertEquals(view.getDescription(), TEST_INVALID_VIEW_INFO.getDescription());
    assertEquals(view.getDefinition().getEntityTypes().size(), 2);
    assertEquals(view.getDefinition().getEntityTypes().get(0), EntityType.DATASET);
    assertEquals(view.getDefinition().getEntityTypes().get(1), EntityType.DASHBOARD);
    assertEquals(view.getDefinition().getFilter().getOperator(), LogicalOperator.OR);
    assertEquals(view.getDefinition().getFilter().getFilters().size(), 0);
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet(),
            Mockito.any(Authentication.class));
    com.linkedin.datahub.graphql.types.view.DataHubViewType type =
        new com.linkedin.datahub.graphql.types.view.DataHubViewType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_VIEW_URN, TEST_VIEW_URN_2), context));
  }
}
