package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class ListLifecycleStagesResolverTest {

  private static final Urn STAGE_URN = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");

  @Test
  public void testListReturnsStages() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(
        new SearchEntityArray(List.of(new SearchEntity().setEntity(STAGE_URN))));
    searchResult.setFrom(0);
    searchResult.setPageSize(1);
    searchResult.setNumEntities(1);
    searchResult.setMetadata(new SearchResultMetadata());
    when(mockClient.search(any(), eq("lifecycleStageType"), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(searchResult);

    when(mockClient.batchGetV2(any(), eq("lifecycleStageType"), any(), any()))
        .thenReturn(
            Map.of(STAGE_URN, LifecycleStageTestUtils.makeStageResponse(STAGE_URN, "Draft", true)));

    ListLifecycleStagesResolver resolver = new ListLifecycleStagesResolver(mockClient);
    List<LifecycleStageType> result = resolver.get(mockEnv).get();

    assertEquals(result.size(), 1);
    LifecycleStageType stage = result.get(0);
    assertEquals(stage.getUrn(), STAGE_URN.toString());
    assertEquals(stage.getName(), "Draft");
    assertTrue(stage.getHideInSearch());
  }

  @Test
  public void testListReturnsEmptyWhenNoStages() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    SearchResult emptyResult = new SearchResult();
    emptyResult.setEntities(new SearchEntityArray());
    emptyResult.setFrom(0);
    emptyResult.setPageSize(0);
    emptyResult.setNumEntities(0);
    emptyResult.setMetadata(new SearchResultMetadata());
    when(mockClient.search(any(), any(), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(emptyResult);

    ListLifecycleStagesResolver resolver = new ListLifecycleStagesResolver(mockClient);
    List<LifecycleStageType> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testListReturnsEmptyWhenSearchReturnsNull() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    when(mockClient.search(any(), any(), any(), any(), any(), eq(0), eq(1000))).thenReturn(null);

    ListLifecycleStagesResolver resolver = new ListLifecycleStagesResolver(mockClient);
    List<LifecycleStageType> result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertTrue(result.isEmpty());
    verify(mockClient, never()).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void testListMultipleStages() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    Urn draftUrn = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");
    Urn deprecatedUrn = UrnUtils.getUrn("urn:li:lifecycleStageType:DEPRECATED");

    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(
        new SearchEntityArray(
            List.of(
                new SearchEntity().setEntity(draftUrn),
                new SearchEntity().setEntity(deprecatedUrn))));
    searchResult.setFrom(0);
    searchResult.setPageSize(2);
    searchResult.setNumEntities(2);
    searchResult.setMetadata(new SearchResultMetadata());
    when(mockClient.search(any(), eq("lifecycleStageType"), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(searchResult);

    when(mockClient.batchGetV2(any(), eq("lifecycleStageType"), any(), any()))
        .thenReturn(
            Map.of(
                draftUrn,
                LifecycleStageTestUtils.makeStageResponse(draftUrn, "Draft", true),
                deprecatedUrn,
                LifecycleStageTestUtils.makeStageResponse(deprecatedUrn, "Deprecated", false)));

    ListLifecycleStagesResolver resolver = new ListLifecycleStagesResolver(mockClient);
    List<LifecycleStageType> result = resolver.get(mockEnv).get();

    assertEquals(result.size(), 2);
    Set<String> names =
        result.stream().map(LifecycleStageType::getName).collect(Collectors.toSet());
    assertTrue(names.contains("Draft"));
    assertTrue(names.contains("Deprecated"));
  }

  @Test
  public void testListSkipsStagesWithoutInfoAspect() throws Exception {
    EntityClient mockClient = mock(EntityClient.class);
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);

    Urn goodUrn = UrnUtils.getUrn("urn:li:lifecycleStageType:DRAFT");
    Urn badUrn = UrnUtils.getUrn("urn:li:lifecycleStageType:ORPHAN");

    SearchResult searchResult = new SearchResult();
    searchResult.setEntities(
        new SearchEntityArray(
            List.of(new SearchEntity().setEntity(goodUrn), new SearchEntity().setEntity(badUrn))));
    searchResult.setFrom(0);
    searchResult.setPageSize(2);
    searchResult.setNumEntities(2);
    searchResult.setMetadata(new SearchResultMetadata());
    when(mockClient.search(any(), eq("lifecycleStageType"), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(searchResult);

    // badUrn has a response but no info aspect
    EntityResponse emptyResponse = new EntityResponse();
    emptyResponse.setUrn(badUrn);
    emptyResponse.setEntityName("lifecycleStageType");
    emptyResponse.setAspects(new EnvelopedAspectMap());

    when(mockClient.batchGetV2(any(), eq("lifecycleStageType"), any(), any()))
        .thenReturn(
            Map.of(
                goodUrn,
                LifecycleStageTestUtils.makeStageResponse(goodUrn, "Draft", true),
                badUrn,
                emptyResponse));

    ListLifecycleStagesResolver resolver = new ListLifecycleStagesResolver(mockClient);
    List<LifecycleStageType> result = resolver.get(mockEnv).get();

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getName(), "Draft");
  }
}
