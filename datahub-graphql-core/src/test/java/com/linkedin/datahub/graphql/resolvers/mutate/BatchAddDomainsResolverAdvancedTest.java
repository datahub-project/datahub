package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAddDomainsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchAddDomainsResolverAdvancedTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_ENTITY_URN_3 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-3,PROD)";
  private static final String TEST_ENTITY_URN_4 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-4,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:marketing";
  private static final String TEST_DOMAIN_3_URN = "urn:li:domain:finance";

  @Test
  public void testGetSuccessLargeBatchMultipleDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN, TEST_DOMAIN_3_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_4, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessResourcesWithMixedExistingDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Entity 1: has domain3
    final Domains domains1 = new Domains();
    final UrnArray urns1 = new UrnArray();
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    domains1.setDomains(urns1);

    // Entity 2: has domain1 and domain3
    final Domains domains2 = new Domains();
    final UrnArray urns2 = new UrnArray();
    urns2.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    urns2.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    domains2.setDomains(urns2);

    // Entity 3: no domains
    // Entity 4: has domain2
    final Domains domains4 = new Domains();
    final UrnArray urns4 = new UrnArray();
    urns4.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    domains4.setDomains(urns4);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(domains1);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(domains2);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_3)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_4)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(domains4);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_4, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetFailureFirstResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureMiddleResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_3)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureLastResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_3)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureFirstDomainDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetFailureLastDomainDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_3_URN)), eq(true)))
        .thenReturn(false);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN, TEST_DOMAIN_3_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetSuccessAllResourcesAlreadyHaveAllDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchAddDomainsResolver resolver = new BatchAddDomainsResolver(mockService, mockClient);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddDomainsInput input =
        new BatchAddDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }
}
