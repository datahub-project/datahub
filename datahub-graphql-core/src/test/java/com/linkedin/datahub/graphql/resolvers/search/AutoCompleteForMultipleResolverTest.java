package com.linkedin.datahub.graphql.resolvers.search;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteMultipleInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;

public class AutoCompleteForMultipleResolverTest {

  private AutoCompleteForMultipleResolverTest() { }

  public static void testAutoCompleteResolverSuccess(
      EntityClient mockClient,
      String entityName,
      EntityType entityType,
      SearchableEntityType<?, ?> entity
  ) throws Exception {
    final AutoCompleteForMultipleResolver resolver = new AutoCompleteForMultipleResolver(ImmutableList.of(entity));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AutoCompleteMultipleInput input = new AutoCompleteMultipleInput();
    input.setQuery("test");
    input.setTypes(ImmutableList.of(entityType));
    input.setLimit(10);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();
    verifyMockEntityClient(
        mockClient,
        entityName,
        "test",
        null,
        10
    );
  }

  // test our main entity types
  @Test
  public static void testAutoCompleteResolverSuccessForDifferentEntities() throws Exception {
    // Daatasets
    EntityClient mockClient = initMockEntityClient(
      Constants.DATASET_ENTITY_NAME,
      "test",
      null,
      10,
      new AutoCompleteResult()
          .setQuery("test")
          .setEntities(new AutoCompleteEntityArray())
          .setSuggestions(new StringArray())
    );
    testAutoCompleteResolverSuccess(mockClient, Constants.DATASET_ENTITY_NAME, EntityType.DATASET, new DatasetType(mockClient));

    // Dashboards
    mockClient = initMockEntityClient(
      Constants.DASHBOARD_ENTITY_NAME,
      "test",
      null,
      10,
      new AutoCompleteResult()
          .setQuery("test")
          .setEntities(new AutoCompleteEntityArray())
          .setSuggestions(new StringArray())
    );
    testAutoCompleteResolverSuccess(mockClient, Constants.DASHBOARD_ENTITY_NAME, EntityType.DASHBOARD, new DashboardType(mockClient));

    //DataFlows
    mockClient = initMockEntityClient(
      Constants.DATA_FLOW_ENTITY_NAME,
      "test",
      null,
      10,
      new AutoCompleteResult()
          .setQuery("test")
          .setEntities(new AutoCompleteEntityArray())
          .setSuggestions(new StringArray())
    );
    testAutoCompleteResolverSuccess(mockClient, Constants.DATA_FLOW_ENTITY_NAME, EntityType.DATA_FLOW, new DataFlowType(mockClient));
  }

  @Test
  public static void testAutoCompleteResolverFailNoQuery() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    final AutoCompleteForMultipleResolver resolver = new AutoCompleteForMultipleResolver(ImmutableList.of(new DatasetType(mockClient)));

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AutoCompleteMultipleInput input = new AutoCompleteMultipleInput();
    // don't set query on input
    input.setTypes(ImmutableList.of(EntityType.DATASET));
    input.setLimit(10);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assert.assertThrows(ValidationException.class, () -> resolver.get(mockEnv).join());
  }

  private static EntityClient initMockEntityClient(
      String entityName,
      String query,
      Filter filters,
      int limit,
      AutoCompleteResult result
  ) throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);
    Mockito.when(client.autoComplete(
        Mockito.eq(entityName),
        Mockito.eq(query),
        Mockito.eq(filters),
        Mockito.eq(limit),
        Mockito.any(Authentication.class)
    )).thenReturn(result);
    return client;
  }

  private static void verifyMockEntityClient(
      EntityClient mockClient,
      String entityName,
      String query,
      Filter filters,
      int limit
  ) throws Exception {
    Mockito.verify(mockClient, Mockito.times(1))
        .autoComplete(
            Mockito.eq(entityName),
            Mockito.eq(query),
            Mockito.eq(filters),
            Mockito.eq(limit),
            Mockito.any(Authentication.class)
        );
  }
}
