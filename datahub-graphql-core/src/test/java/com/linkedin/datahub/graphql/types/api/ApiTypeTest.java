package com.linkedin.datahub.graphql.types.api;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Api;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ApiKey;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import graphql.execution.DataFetcherResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ApiTypeTest {

  private static final String API_URN_1 = "urn:li:api:payments.charge";
  private static final String API_URN_2 = "urn:li:api:payments.refund";

  private static QueryContext mockContext() {
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());
    return context;
  }

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn urn1 = Urn.createFromString(API_URN_1);
    Urn urn2 = Urn.createFromString(API_URN_2);

    ApiKey key = new ApiKey();
    key.setId("payments.charge");
    Status status = new Status().setRemoved(false);

    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.API_KEY_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(key.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.API_ENTITY_NAME),
                Mockito.eq(ImmutableSet.of(urn1, urn2)),
                any()))
        .thenReturn(
            ImmutableMap.of(
                urn1,
                new EntityResponse()
                    .setEntityName(Constants.API_ENTITY_NAME)
                    .setUrn(urn1)
                    .setAspects(new EnvelopedAspectMap(aspects))));

    ApiType type = new ApiType(client);
    List<DataFetcherResult<Api>> result =
        type.batchLoad(ImmutableList.of(API_URN_1, API_URN_2), mockContext());

    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.API_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(urn1, urn2)),
            any());

    assertEquals(result.size(), 2);

    Api api = result.get(0).getData();
    assertEquals(api.getUrn(), API_URN_1);
    assertEquals(api.getType(), EntityType.API);

    // urn2 was not in the response, so its slot is null
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());

    ApiType type = new ApiType(mockClient);
    QueryContext context = mockContext();

    assertThrows(
        RuntimeException.class, () -> type.batchLoad(ImmutableList.of(API_URN_1), context));
  }

  @Test
  public void testAutoComplete() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    AutoCompleteResult autoCompleteResult =
        new AutoCompleteResult()
            .setQuery("payments")
            .setSuggestions(new StringArray())
            .setEntities(new AutoCompleteEntityArray());

    Mockito.when(
            mockClient.autoComplete(
                any(),
                Mockito.eq(Constants.API_ENTITY_NAME),
                Mockito.eq("payments"),
                Mockito.any(),
                Mockito.eq(10)))
        .thenReturn(autoCompleteResult);

    ApiType type = new ApiType(mockClient);
    AutoCompleteResults results =
        type.autoComplete("payments", null, (Filter) null, 10, mockContext());

    assertNotNull(results);
    assertEquals(results.getQuery(), "payments");
    Mockito.verify(mockClient, Mockito.times(1))
        .autoComplete(
            any(),
            Mockito.eq(Constants.API_ENTITY_NAME),
            Mockito.eq("payments"),
            Mockito.any(),
            Mockito.eq(10));
  }

  @Test
  public void testType() {
    ApiType type = new ApiType(Mockito.mock(EntityClient.class));
    assertEquals(type.type(), EntityType.API);
  }
}
