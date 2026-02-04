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
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DomainsResolversIntegrationTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:marketing";
  private static final String TEST_DOMAIN_3_URN = "urn:li:domain:finance";

  @Test
  public void testAddThenRemoveWorkflow() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Initially no domains
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);

    AddDomainsResolver addResolver = new AddDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(addResolver.get(mockEnv).get());
  }

  @Test
  public void testAddMultipleTimes() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Start with domain1
    final Domains initialDomains = new Domains();
    final UrnArray initialUrns = new UrnArray();
    initialUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    initialDomains.setDomains(initialUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(initialDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_3_URN)), eq(true)))
        .thenReturn(true);

    AddDomainsResolver addResolver = new AddDomainsResolver(mockClient, mockService);
    QueryContext mockContext = getMockAllowContext();

    // Add domain2
    DataFetchingEnvironment mockEnv1 = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv1.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv1.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_2_URN));
    Mockito.when(mockEnv1.getContext()).thenReturn(mockContext);

    assertTrue(addResolver.get(mockEnv1).get());

    // Add domain3
    DataFetchingEnvironment mockEnv2 = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv2.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv2.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_3_URN));
    Mockito.when(mockEnv2.getContext()).thenReturn(mockContext);

    assertTrue(addResolver.get(mockEnv2).get());
  }

  @Test
  public void testRemoveFromComplexDomainSet() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Start with multiple domains
    final Domains initialDomains = new Domains();
    final UrnArray initialUrns = new UrnArray();
    initialUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    initialUrns.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    initialUrns.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    initialUrns.add(UrnUtils.getUrn("urn:li:domain:sales"));
    initialUrns.add(UrnUtils.getUrn("urn:li:domain:operations"));
    initialDomains.setDomains(initialUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(initialDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);

    RemoveDomainsResolver removeResolver = new RemoveDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_3_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(removeResolver.get(mockEnv).get());
  }

  @Test
  public void testAddAndRemoveIdempotency() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Add same domain twice - should be idempotent
    final Domains initialDomains = new Domains();
    final UrnArray initialUrns = new UrnArray();
    initialUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    initialDomains.setDomains(initialUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq("domains"),
                Mockito.eq(0L)))
        .thenReturn(initialDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);

    AddDomainsResolver addResolver = new AddDomainsResolver(mockClient, mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrns")))
        .thenReturn(ImmutableList.of(TEST_DOMAIN_1_URN));
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(addResolver.get(mockEnv).get());
  }
}
