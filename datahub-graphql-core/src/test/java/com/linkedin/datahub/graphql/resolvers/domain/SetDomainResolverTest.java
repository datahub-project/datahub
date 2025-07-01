package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SetDomainResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_EXISTING_DOMAIN_URN = "urn:li:domain:test-id";
  private static final String TEST_NEW_DOMAIN_URN = "urn:li:domain:test-id-2";

  @Test
  public void testGetSuccessNoExistingDomains() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true)))
        .thenReturn(true);

    SetDomainResolver resolver = new SetDomainResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Domains newDomains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_NEW_DOMAIN_URN))));
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), DOMAINS_ASPECT_NAME, newDomains);

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true));
  }

  @Test
  public void testGetSuccessExistingDomains() throws Exception {
    Domains originalDomains =
        new Domains()
            .setDomains(
                new UrnArray(ImmutableList.of(Urn.createFromString(TEST_EXISTING_DOMAIN_URN))));

    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(
                        new EnvelopedAspectMap(
                            ImmutableMap.of(
                                Constants.DOMAINS_ASPECT_NAME,
                                new EnvelopedAspect()
                                    .setValue(new Aspect(originalDomains.data())))))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true)))
        .thenReturn(true);

    SetDomainResolver resolver = new SetDomainResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).get();

    final Domains newDomains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_NEW_DOMAIN_URN))));
    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            UrnUtils.getUrn(TEST_ENTITY_URN), DOMAINS_ASPECT_NAME, newDomains);

    verifyIngestProposal(mockClient, 1, proposal);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), Mockito.eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true));
  }

  @Test
  public void testGetFailureDomainDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true)))
        .thenReturn(false);

    SetDomainResolver resolver = new SetDomainResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Test setting the domain
    Mockito.when(
            mockClient.batchGetV2(
                any(),
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(Urn.createFromString(TEST_ENTITY_URN)))),
                Mockito.eq(ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                Urn.createFromString(TEST_ENTITY_URN),
                new EntityResponse()
                    .setEntityName(Constants.DATASET_ENTITY_NAME)
                    .setUrn(Urn.createFromString(TEST_ENTITY_URN))
                    .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))));

    EntityService<?> mockService = getMockEntityService();
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_NEW_DOMAIN_URN)), eq(true)))
        .thenReturn(true);

    SetDomainResolver resolver = new SetDomainResolver(mockClient, mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    SetDomainResolver resolver = new SetDomainResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), anyBoolean());
    SetDomainResolver resolver =
        new SetDomainResolver(mockClient, Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("entityUrn"))).thenReturn(TEST_ENTITY_URN);
    Mockito.when(mockEnv.getArgument(Mockito.eq("domainUrn"))).thenReturn(TEST_NEW_DOMAIN_URN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
