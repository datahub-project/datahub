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
import com.linkedin.datahub.graphql.generated.BatchRemoveDomainsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.domain.Domains;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveDomainsResolverAdvancedTest {

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

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
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
  public void testGetSuccessResourcesWithDifferentDomainSets() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // Entity 1: has all three domains
    final Domains domains1 = new Domains();
    final UrnArray urns1 = new UrnArray();
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    domains1.setDomains(urns1);

    // Entity 2: has domain1 only
    final Domains domains2 = new Domains();
    final UrnArray urns2 = new UrnArray();
    urns2.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    domains2.setDomains(urns2);

    // Entity 3: has domain2 and domain3
    final Domains domains3 = new Domains();
    final UrnArray urns3 = new UrnArray();
    urns3.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    urns3.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    domains3.setDomains(urns3);

    // Entity 4: no domains
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
        .thenReturn(domains3);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_4)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
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

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
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

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_3)), eq(true)))
        .thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
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

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_3)), eq(true)))
        .thenReturn(false);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
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
  public void testGetSuccessRemoveAllDomainsFromAllResources() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessRemoveNonExistentDomainsFromResources() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // All resources have domain3, but we're trying to remove domain1 and domain2
    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessPartialRemovalLeavingOtherDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // Resources have domain1, domain2, and domain3
    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(), Mockito.any(Urn.class), Mockito.eq(DOMAINS_ASPECT_NAME), Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessRemoveMultipleDomainsFromMixedResources() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // Entity 1: has domain1 and domain2
    final Domains domains1 = new Domains();
    final UrnArray urns1 = new UrnArray();
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    urns1.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    domains1.setDomains(urns1);

    // Entity 2: has only domain2
    final Domains domains2 = new Domains();
    final UrnArray urns2 = new UrnArray();
    urns2.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    domains2.setDomains(urns2);

    // Entity 3: has only domain1
    final Domains domains3 = new Domains();
    final UrnArray urns3 = new UrnArray();
    urns3.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    domains3.setDomains(urns3);

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
        .thenReturn(domains3);

    Mockito.when(mockService.exists(any(), Mockito.any(Urn.class), eq(true))).thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }
}
