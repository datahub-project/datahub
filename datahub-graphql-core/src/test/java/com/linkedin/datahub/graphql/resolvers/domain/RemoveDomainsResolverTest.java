package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoveDomainsResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:marketing";

  @Test
  public void testGetSuccessNoExistingDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    RemoveDomainsResolver resolver = new RemoveDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessExistingDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Domains originalDomains = new Domains();
    final UrnArray existingDomains = new UrnArray();
    existingDomains.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomains.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingDomains.add(UrnUtils.getUrn("urn:li:domain:finance"));
    originalDomains.setDomains(existingDomains);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(originalDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    RemoveDomainsResolver resolver = new RemoveDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetSuccessRemoveNonExistentDomain() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Domains originalDomains = new Domains();
    final UrnArray existingDomains = new UrnArray();
    existingDomains.add(UrnUtils.getUrn("urn:li:domain:finance"));
    originalDomains.setDomains(existingDomains);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(originalDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    RemoveDomainsResolver resolver = new RemoveDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);

    RemoveDomainsResolver resolver = new RemoveDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    RemoveDomainsResolver resolver = new RemoveDomainsResolver(mockClient, mockService);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
