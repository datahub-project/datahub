package com.linkedin.datahub.graphql.resolvers.glossary;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateParentNodeInput;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateParentNodeResolver;
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

import java.net.URISyntaxException;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class UpdateParentNodeResolverTest {

  private static final String CONTAINER_URN = "urn:li:container:00005397daf94708a8822b8106cfd451";
  private static final String PARENT_NODE_URN = "urn:li:glossaryNode:00005397daf94708a8822b8106cfd451";
  private static final String TERM_URN = "urn:li:glossaryTerm:11115397daf94708a8822b8106cfd451";
  private static final String NODE_URN = "urn:li:glossaryNode:22225397daf94708a8822b8106cfd451";
  private static final UpdateParentNodeInput INPUT = new UpdateParentNodeInput(PARENT_NODE_URN, TERM_URN);
  private static final UpdateParentNodeInput INPUT_WITH_NODE = new UpdateParentNodeInput(PARENT_NODE_URN, NODE_URN);
  private static final UpdateParentNodeInput INVALID_INPUT = new UpdateParentNodeInput(CONTAINER_URN, TERM_URN);
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
    info.setName(name);
    info.setParentNode(GlossaryNodeUrn.createFromString(PARENT_NODE_URN));
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(true);
    Mockito.when(mockService.exists(GlossaryNodeUrn.createFromString(PARENT_NODE_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService);
    final MetadataChangeProposal proposal = setupTests(mockEnv, mockService);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1, proposal);
  }

  @Test
  public void testGetSuccessForNode() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(NODE_URN))).thenReturn(true);
    Mockito.when(mockService.exists(GlossaryNodeUrn.createFromString(PARENT_NODE_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT_WITH_NODE);

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
    info.setName(name);
    info.setParentNode(GlossaryNodeUrn.createFromString(PARENT_NODE_URN));
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(info));
    proposal.setChangeType(ChangeType.UPSERT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService);

    assertTrue(resolver.get(mockEnv).get());
    verifyIngestProposal(mockService, 1, proposal);
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(false);
    Mockito.when(mockService.exists(GlossaryNodeUrn.createFromString(PARENT_NODE_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService);
    setupTests(mockEnv, mockService);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureNodeDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(true);
    Mockito.when(mockService.exists(GlossaryNodeUrn.createFromString(PARENT_NODE_URN))).thenReturn(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService);
    setupTests(mockEnv, mockService);

    assertThrows(IllegalArgumentException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureParentIsNotNode() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.exists(Urn.createFromString(TERM_URN))).thenReturn(true);
    Mockito.when(mockService.exists(GlossaryNodeUrn.createFromString(PARENT_NODE_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INVALID_INPUT);

    UpdateParentNodeResolver resolver = new UpdateParentNodeResolver(mockService);
    setupTests(mockEnv, mockService);

    assertThrows(URISyntaxException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }
}
