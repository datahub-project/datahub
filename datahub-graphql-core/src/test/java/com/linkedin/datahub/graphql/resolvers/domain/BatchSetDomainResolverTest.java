package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSetDomainInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchSetDomainResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchSetDomainResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_DOMAIN_1_URN = "urn:li:domain:test-id-1";
  private static final String TEST_DOMAIN_2_URN = "urn:li:domain:test-id-2";

  @Test
  public void testGetSuccessNoExistingDomains() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_2_URN))).thenReturn(true);

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            TEST_DOMAIN_2_URN,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final Domains newDomains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_DOMAIN_2_URN))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), DOMAINS_ASPECT_NAME, newDomains);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), DOMAINS_ASPECT_NAME, newDomains);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_DOMAIN_2_URN)));
  }

  @Test
  public void testGetSuccessExistingDomains() throws Exception {
    final Domains originalDomain =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_DOMAIN_1_URN))));

    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalDomain);

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalDomain);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_2_URN))).thenReturn(true);

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            TEST_DOMAIN_2_URN,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final Domains newDomains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_DOMAIN_2_URN))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), DOMAINS_ASPECT_NAME, newDomains);
    proposal1.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN_1));
    proposal1.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal1.setAspectName(Constants.DOMAINS_ASPECT_NAME);
    proposal1.setAspect(GenericRecordUtils.serializeAspect(newDomains));
    proposal1.setChangeType(ChangeType.UPSERT);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), DOMAINS_ASPECT_NAME, newDomains);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_DOMAIN_2_URN)));
  }

  @Test
  public void testGetSuccessUnsetDomains() throws Exception {
    final Domains originalDomain =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(Urn.createFromString(TEST_DOMAIN_1_URN))));

    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalDomain);

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalDomain);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_2_URN))).thenReturn(true);

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final Domains newDomains = new Domains().setDomains(new UrnArray(ImmutableList.of()));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), DOMAINS_ASPECT_NAME, newDomains);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), DOMAINS_ASPECT_NAME, newDomains);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));
  }

  @Test
  public void testGetFailureDomainDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_1_URN))).thenReturn(false);

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                Mockito.eq(Constants.DOMAINS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_1))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN_2))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_DOMAIN_1_URN))).thenReturn(true);

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = getMockEntityService();

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());

    BatchSetDomainResolver resolver = new BatchSetDomainResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchSetDomainInput input =
        new BatchSetDomainInput(
            null,
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
