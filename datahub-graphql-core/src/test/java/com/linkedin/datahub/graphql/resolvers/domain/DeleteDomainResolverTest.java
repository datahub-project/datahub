package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteDomainResolverTest {

  private static final String TEST_URN = "urn:li:domain:test-id";
  private static final String CHILD_URN = "urn:li:domain:child-id";

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Domain has 0 child domains -- early exit before filterExistingUrns.
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(200)))
        .thenReturn(new SearchResult().setNumEntities(0).setEntities(new SearchEntityArray()));

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(Urn.createFromString(TEST_URN)));
  }

  @Test
  public void testDeleteWithChildDomains() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // OpenSearch returns one child candidate.
    Urn childUrn = UrnUtils.getUrn(CHILD_URN);
    SearchEntity childEntity = new SearchEntity().setEntity(childUrn);
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(200)))
        .thenReturn(
            new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(childEntity)));

    // Primary store (MySQL) confirms the child still exists.
    Mockito.when(mockClient.filterExistingUrns(any(), Mockito.eq(Set.of(childUrn))))
        .thenReturn(Set.of(childUrn));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }

  @Test
  public void testDeleteBlockedWhenPagedCandidatesAllStaleButMoreExist() throws Exception {
    // When OpenSearch reports more total candidates than fit in one page and all
    // fetched candidates are stale in MySQL, deletion must still be blocked.
    // Without the fallback check (numEntities > entities.size()), the code would
    // incorrectly return false -- potentially allowing deletion of a domain that
    // still has real children in the un-fetched remainder of the OpenSearch result.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // OpenSearch reports 300 total but only returns one entry in this page,
    // simulating the case where numEntities > the fetched page size.
    Urn childUrn = UrnUtils.getUrn(CHILD_URN);
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(200)))
        .thenReturn(
            new SearchResult()
                .setNumEntities(300)
                .setEntities(new SearchEntityArray(new SearchEntity().setEntity(childUrn))));

    // The fetched candidate is stale in MySQL.
    Mockito.when(mockClient.filterExistingUrns(any(), any())).thenReturn(Collections.emptySet());

    // Must still block deletion: we cannot confirm childlessness from one page.
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }

  @Test
  public void testDeleteWithStaleChildDomains() throws Exception {
    // Regression test for the OpenSearch eventual-consistency race condition:
    // OpenSearch still shows a child that was just deleted from MySQL.
    // hasChildDomains() must allow the parent delete to proceed once the
    // primary store (MySQL) confirms no child actually exists.
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // OpenSearch returns a stale child candidate.
    Urn childUrn = UrnUtils.getUrn(CHILD_URN);
    SearchEntity childEntity = new SearchEntity().setEntity(childUrn);
    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq("domain"),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(0),
                Mockito.eq(200)))
        .thenReturn(
            new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(childEntity)));

    // Primary store (MySQL) confirms the child no longer exists -- stale OpenSearch hit.
    Mockito.when(mockClient.filterExistingUrns(any(), Mockito.eq(Set.of(childUrn))))
        .thenReturn(Collections.emptySet());

    // Deletion should succeed because the only OpenSearch candidate is stale.
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(Urn.createFromString(TEST_URN)));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }
}
