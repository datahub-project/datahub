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
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveDomainsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:engineering";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:marketing";

  @Test
  public void testGetSuccessNoExistingDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);

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
  public void testGetSuccessExistingDomains() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final Domains existingDomains1 = new Domains();
    final UrnArray existingDomainUrns1 = new UrnArray();
    existingDomainUrns1.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomainUrns1.add(UrnUtils.getUrn(TEST_DOMAIN_2_URN));
    existingDomains1.setDomains(existingDomainUrns1);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingDomains1);

    final Domains existingDomains2 = new Domains();
    final UrnArray existingDomainUrns2 = new UrnArray();
    existingDomainUrns2.add(UrnUtils.getUrn(TEST_DOMAIN_1_URN));
    existingDomains2.setDomains(existingDomainUrns2);

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(existingDomains2);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_2_URN)), eq(true)))
        .thenReturn(true);

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
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_DOMAIN_1_URN)), eq(true)))
        .thenReturn(true);

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

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    BatchRemoveDomainsResolver resolver = new BatchRemoveDomainsResolver(mockService, null);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveDomainsInput input =
        new BatchRemoveDomainsInput(
            ImmutableList.of(TEST_DOMAIN_1_URN, TEST_DOMAIN_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }
}
