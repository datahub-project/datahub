package com.linkedin.datahub.graphql.resolvers.glossary;


import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateNameInput;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateNameResolver;
import com.linkedin.domain.DomainProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.concurrent.CompletionException;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class UpdateNameResolverTest {

  private static final String NEW_NAME = "New Name";
  private static final String TERM_URN = "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  private static final String NODE_URN = "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451";
  private static final String DOMAIN_URN = "urn:li:domain:22225397daf94708a8822b8106cfd451";
  private static final UpdateNameInput INPUT = new UpdateNameInput(NEW_NAME, TERM_URN);
  private static final UpdateNameInput INPUT_FOR_NODE = new UpdateNameInput(NEW_NAME, NODE_URN);
  private static final UpdateNameInput INPUT_FOR_DOMAIN = new UpdateNameInput(NEW_NAME, DOMAIN_URN);
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  private MetadataChangeProposal setupTests(DataFetchingEnvironment mockEnv, EntityService mockService) throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(mockService.getAspect(
            Urn.createFromString(TERM_URN),
            Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
            0))
        .thenReturn(new GlossaryTermInfo().setName(name));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(TERM_URN));
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    GlossaryTermInfo info = new GlossaryTermInfo();
    info.setName(NEW_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateNameResolver resolver = new UpdateNameResolver(mockService);
    final MetadataChangeProposal proposal = setupTests(mockEnv, mockService);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any()
    );
  }

  @Test
  public void testGetSuccessForNode() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(NODE_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_FOR_NODE);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(mockService.getAspect(
            Urn.createFromString(NODE_URN),
            Constants.GLOSSARY_NODE_INFO_ASPECT_NAME,
            0))
        .thenReturn(new GlossaryNodeInfo().setName(name));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(NODE_URN));
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    GlossaryNodeInfo info = new GlossaryNodeInfo();
    info.setName(NEW_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    UpdateNameResolver resolver = new UpdateNameResolver(mockService);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any()
    );
  }

  @Test
  public void testGetSuccessForDomain() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(DOMAIN_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_FOR_DOMAIN);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(mockService.getAspect(
            Urn.createFromString(DOMAIN_URN),
            Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
            0))
        .thenReturn(new DomainProperties().setName(name));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(Urn.createFromString(DOMAIN_URN));
    proposal.setEntityType(Constants.DOMAIN_ENTITY_NAME);
    DomainProperties properties = new DomainProperties();
    properties.setName(NEW_NAME);
    proposal.setAspectName(Constants.DOMAIN_PROPERTIES_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(properties));
    proposal.setChangeType(ChangeType.UPSERT);

    UpdateNameResolver resolver = new UpdateNameResolver(mockService);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any()
    );
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateNameResolver resolver = new UpdateNameResolver(mockService);
    setupTests(mockEnv, mockService);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any());
  }
}
