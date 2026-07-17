package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteDomainResolverTest {

  private static final String TEST_URN = "urn:li:domain:test-id";
  private static final String CHILD_URN = "urn:li:domain:child-id";
  private static final Urn TEST_DOMAIN = UrnUtils.getUrn(TEST_URN);
  private static final Urn CHILD_DOMAIN = UrnUtils.getUrn(CHILD_URN);

  private void mockHasChildDomains(
      EntityClient mockClient, QueryContext queryContext, boolean hasChildren)
      throws RemoteInvocationException, URISyntaxException {
    if (hasChildren) {
      when(mockClient.filter(
              eq(queryContext.getOperationContext()),
              eq(DOMAIN_ENTITY_NAME),
              any(),
              eq(null),
              eq(0),
              eq(200)))
          .thenReturn(
              new SearchResult()
                  .setEntities(new SearchEntityArray(new SearchEntity().setEntity(CHILD_DOMAIN)))
                  .setNumEntities(1));
      when(mockClient.batchGetV2(
              eq(queryContext.getOperationContext()),
              eq(DOMAIN_ENTITY_NAME),
              eq(Set.of(CHILD_DOMAIN)),
              eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
          .thenReturn(Map.of(CHILD_DOMAIN, domainResponse(CHILD_DOMAIN, TEST_DOMAIN)));
    } else {
      when(mockClient.filter(
              eq(queryContext.getOperationContext()),
              eq(DOMAIN_ENTITY_NAME),
              any(),
              eq(null),
              eq(0),
              eq(200)))
          .thenReturn(new SearchResult().setEntities(new SearchEntityArray()).setNumEntities(0));
    }
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext queryContext = getMockAllowContext();
    mockHasChildDomains(mockClient, queryContext, false);
    Mockito.when(mockEnv.getContext()).thenReturn(queryContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(), Mockito.eq(Urn.createFromString(TEST_URN)));
  }

  @Test
  public void testDeleteWithChildDomains() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext queryContext = getMockAllowContext();
    mockHasChildDomains(mockClient, queryContext, true);
    Mockito.when(mockEnv.getContext()).thenReturn(queryContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0)).deleteEntity(any(), Mockito.any());
  }

  @Test
  public void testDeleteAfterChildRemovedFromPrimaryStore() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DeleteDomainResolver resolver = new DeleteDomainResolver(mockClient);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_URN);
    QueryContext queryContext = getMockAllowContext();
    when(mockClient.filter(
            eq(queryContext.getOperationContext()),
            eq(DOMAIN_ENTITY_NAME),
            any(),
            eq(null),
            eq(0),
            eq(200)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(new SearchEntity().setEntity(CHILD_DOMAIN)))
                .setNumEntities(1));
    when(mockClient.batchGetV2(
            eq(queryContext.getOperationContext()),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD_DOMAIN)),
            eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Collections.emptyMap());
    Mockito.when(mockEnv.getContext()).thenReturn(queryContext);

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

  private static EntityResponse domainResponse(Urn urn, Urn parentUrn) {
    DomainProperties properties = new DomainProperties();
    properties.setParentDomain(parentUrn, SetMode.IGNORE_NULL);
    properties.setName("test-domain");
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(properties.data()))
            .setCreated(
                new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));
    return new EntityResponse().setUrn(urn).setAspects(aspectMap);
  }
}
