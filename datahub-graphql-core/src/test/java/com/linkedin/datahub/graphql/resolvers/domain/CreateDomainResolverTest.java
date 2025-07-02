package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateDomainInput;
import com.linkedin.datahub.graphql.generated.OwnerEntityType;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateDomainResolverTest {

  private static final Urn TEST_DOMAIN_URN = Urn.createFromTuple("domain", "test-id");
  private static final Urn TEST_PARENT_DOMAIN_URN = Urn.createFromTuple("domain", "test-parent-id");
  private static final Urn TEST_OWNER_URN = UrnUtils.getUrn("urn:li:corpuser:test-owner");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final CreateDomainInput TEST_INPUT =
      new CreateDomainInput(
          "test-id", "test-name", "test-description", TEST_PARENT_DOMAIN_URN.toString(), null);

  private static final CreateDomainInput TEST_INPUT_NO_PARENT_DOMAIN =
      new CreateDomainInput("test-id", "test-name", "test-description", null, null);

  private static final OwnerInput TEST_OWNER_INPUT =
      new OwnerInput(
          TEST_OWNER_URN.toString(),
          OwnerEntityType.CORP_USER,
          com.linkedin.datahub.graphql.generated.OwnershipType.TECHNICAL_OWNER,
          null);

  private static final CreateDomainInput TEST_INPUT_WITH_OWNERS =
      new CreateDomainInput(
          "test-id",
          "test-name",
          "test-description",
          TEST_PARENT_DOMAIN_URN.toString(),
          ImmutableList.of(TEST_OWNER_INPUT));

  private static final CreateDomainInput TEST_INPUT_NO_PARENT_WITH_OWNERS =
      new CreateDomainInput(
          "test-id", "test-name", "test-description", null, ImmutableList.of(TEST_OWNER_INPUT));

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_PARENT_DOMAIN_URN))).thenReturn(true);

    // Mock the domain URN that will be returned from ingestProposal
    Mockito.when(mockClient.ingestProposal(any(), any(), Mockito.eq(false)))
        .thenReturn(TEST_DOMAIN_URN.toString());

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        TEST_INPUT.getName(), TEST_PARENT_DOMAIN_URN)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    resolver.get(mockEnv).get();

    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    props.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    props.setParentDomain(TEST_PARENT_DOMAIN_URN);
    proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(props));
    proposal.setChangeType(ChangeType.UPSERT);

    // Not ideal to match against "any", but we don't know the auto-generated execution request id
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(), Mockito.argThat(new CreateDomainProposalMatcher(proposal)), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoParentDomain() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);

    // Mock the domain URN that will be returned from ingestProposal
    Mockito.when(mockClient.ingestProposal(any(), any(), Mockito.eq(false)))
        .thenReturn(TEST_DOMAIN_URN.toString());

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_NO_PARENT_DOMAIN);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(DomainUtils.buildNameAndParentDomainFilter(TEST_INPUT.getName(), null)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    resolver.get(mockEnv).get();

    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    props.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(props));
    proposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(), Mockito.argThat(new CreateDomainProposalMatcher(proposal)), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessWithOwners() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);
    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_PARENT_DOMAIN_URN))).thenReturn(true);
    Mockito.when(mockService.exists(any(), Mockito.eq(TEST_OWNER_URN), eq(true))).thenReturn(true);

    // Mock the domain URN that will be returned from ingestProposal
    Mockito.when(mockClient.ingestProposal(any(), any(), Mockito.eq(false)))
        .thenReturn(TEST_DOMAIN_URN.toString());

    // Mock ownership type URNs that OwnerUtils.addOwnersToResources checks for
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner")),
                Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__none")),
                Mockito.eq(true)))
        .thenReturn(true);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT_WITH_OWNERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        TEST_INPUT_WITH_OWNERS.getName(), TEST_PARENT_DOMAIN_URN)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    resolver.get(mockEnv).get();

    // Verify domain creation
    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal domainProposal = new MetadataChangeProposal();
    domainProposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    domainProposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    props.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    props.setParentDomain(TEST_PARENT_DOMAIN_URN);
    domainProposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    domainProposal.setAspect(GenericRecordUtils.serializeAspect(props));
    domainProposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(new CreateDomainProposalMatcher(domainProposal)),
            Mockito.eq(false));

    // Verify owners are added
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(any(), any(), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoParentDomainWithOwners() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);
    Mockito.when(mockService.exists(any(), Mockito.eq(TEST_OWNER_URN), eq(true))).thenReturn(true);

    // Mock the domain URN that will be returned from ingestProposal
    Mockito.when(mockClient.ingestProposal(any(), any(), Mockito.eq(false)))
        .thenReturn(TEST_DOMAIN_URN.toString());

    // Mock ownership type URNs that OwnerUtils.addOwnersToResources checks for
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner")),
                Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__none")),
                Mockito.eq(true)))
        .thenReturn(true);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input")))
        .thenReturn(TEST_INPUT_NO_PARENT_WITH_OWNERS);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(DomainUtils.buildNameAndParentDomainFilter(TEST_INPUT.getName(), null)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    resolver.get(mockEnv).get();

    // Verify domain creation
    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal domainProposal = new MetadataChangeProposal();
    domainProposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    domainProposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    props.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    domainProposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    domainProposal.setAspect(GenericRecordUtils.serializeAspect(props));
    domainProposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(new CreateDomainProposalMatcher(domainProposal)),
            Mockito.eq(false));

    // Verify owners are added - TODO Verify the contents.
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(any(), any(), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoOwnersProvided() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);
    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_PARENT_DOMAIN_URN))).thenReturn(true);

    // Mock the domain URN that will be returned from ingestProposal
    Mockito.when(mockClient.ingestProposal(any(), any(), Mockito.eq(false)))
        .thenReturn(TEST_DOMAIN_URN.toString());

    // Mock ownership type URNs that OwnerUtils.addActorAsOwner checks for
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner")),
                Mockito.eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(
                any(),
                Mockito.eq(UrnUtils.getUrn("urn:li:ownershipType:__system__none")),
                Mockito.eq(true)))
        .thenReturn(true);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        TEST_INPUT.getName(), TEST_PARENT_DOMAIN_URN)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    resolver.get(mockEnv).get();

    // Verify domain creation
    final DomainKey key = new DomainKey();
    key.setId("test-id");
    final MetadataChangeProposal domainProposal = new MetadataChangeProposal();
    domainProposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    domainProposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties props = new DomainProperties();
    props.setDescription("test-description");
    props.setName("test-name");
    props.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    props.setParentDomain(TEST_PARENT_DOMAIN_URN);
    domainProposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    domainProposal.setAspect(GenericRecordUtils.serializeAspect(props));
    domainProposal.setChangeType(ChangeType.UPSERT);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.argThat(new CreateDomainProposalMatcher(domainProposal)),
            Mockito.eq(false));

    // Verify current user is added as owner when no owners provided
    // The EntityService.ingestProposal is called with AspectsBatch, not individual proposals
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(any(), any(), Mockito.eq(false));
  }

  @Test
  public void testGetInvalidParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_PARENT_DOMAIN_URN))).thenReturn(false);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetNameConflict() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_DOMAIN_URN))).thenReturn(false);

    Mockito.when(mockClient.exists(any(), Mockito.eq(TEST_PARENT_DOMAIN_URN))).thenReturn(true);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockClient.filter(
                any(),
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        TEST_INPUT.getName(), TEST_PARENT_DOMAIN_URN)),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(new SearchEntity().setEntity(TEST_DOMAIN_URN))));

    DomainProperties domainProperties = new DomainProperties();
    domainProperties.setDescription(TEST_INPUT.getDescription());
    domainProperties.setName(TEST_INPUT.getName());
    domainProperties.setCreated(new AuditStamp().setActor(TEST_ACTOR_URN).setTime(0L));
    domainProperties.setParentDomain(TEST_PARENT_DOMAIN_URN);

    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(domainProperties.data())));
    entityResponse.setAspects(envelopedAspectMap);

    Map<Urn, EntityResponse> entityResponseMap = new HashMap<>();
    entityResponseMap.put(TEST_DOMAIN_URN, entityResponse);

    Mockito.when(
            mockClient.batchGetV2(
                any(), Mockito.eq(Constants.DOMAIN_ENTITY_NAME), Mockito.any(), Mockito.any()))
        .thenReturn(entityResponseMap);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(any(), Mockito.any(), Mockito.eq(false));
    CreateDomainResolver resolver = new CreateDomainResolver(mockClient, mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
