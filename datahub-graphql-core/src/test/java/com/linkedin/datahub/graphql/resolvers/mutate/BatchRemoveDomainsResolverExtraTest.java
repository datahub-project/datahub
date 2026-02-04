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

public class BatchRemoveDomainsResolverExtraTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_ENTITY_URN_3 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-3,PROD)";
  private static final String TEST_ENTITY_URN_4 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-4,PROD)";
  private static final String TEST_ENTITY_URN_5 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-5,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:marketing";
  private static final String TEST_DOMAIN_3_URN = "urn:li:domain:finance";
  private static final String TEST_DOMAIN_4_URN = "urn:li:domain:sales";
  private static final String TEST_DOMAIN_5_URN = "urn:li:domain:operations";

  @Test
  public void testGetSuccessSingleDomainFromManyResources() throws Exception {
    EntityService<?> mockService = getMockEntityService();

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

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_3, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_4, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_5, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessManyDomainsFromSingleResource() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_3_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_4_URN));
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_5_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(
                TEST_DOMAIN_1_URN,
                TEST_DOMAIN_2_URN,
                TEST_DOMAIN_3_URN,
                TEST_DOMAIN_4_URN,
                TEST_DOMAIN_5_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetFailureUnauthorizedForOneResourceInBatch() throws Exception {
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

    QueryContext mockContext = getMockDenyContext();
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
  public void testGetSuccessDuplicateResourcesInInput() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessDuplicateDomainsInInput() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final Domains existingDomains = new Domains();
    final UrnArray existingUrns = new UrnArray();
    existingUrns.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomains.setDomains(existingUrns);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingDomains);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_1_URN, TEST_DOMAIN_1_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1);
  }
}
