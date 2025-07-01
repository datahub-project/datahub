package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static com.linkedin.datahub.graphql.TestUtils.verifyIngestProposal;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateGlossaryNodeResolverTest {

  private static final CreateGlossaryEntityInput TEST_INPUT =
      new CreateGlossaryEntityInput(
          "test-id",
          "test-name",
          "test-description",
          "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");
  private static final CreateGlossaryEntityInput TEST_INPUT_NO_DESCRIPTION =
      new CreateGlossaryEntityInput(
          "test-id", "test-name", null, "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");

  private static final CreateGlossaryEntityInput TEST_INPUT_NO_PARENT_NODE =
      new CreateGlossaryEntityInput("test-id", "test-name", "test-description", null);

  private final String parentNodeUrn = "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";

  private MetadataChangeProposal setupTest(
      DataFetchingEnvironment mockEnv,
      CreateGlossaryEntityInput input,
      String description,
      String parentNode)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final GlossaryNodeKey key = new GlossaryNodeKey();
    key.setName("test-id");
    GlossaryNodeInfo props = new GlossaryNodeInfo();
    props.setDefinition(description);
    props.setName("test-name");
    if (parentNode != null) {
      final GlossaryNodeUrn parent = GlossaryNodeUrn.createFromString(parentNode);
      props.setParentNode(parent);
    }
    return MutationUtils.buildMetadataChangeProposalWithKey(
        key, GLOSSARY_NODE_ENTITY_NAME, GLOSSARY_NODE_INFO_ASPECT_NAME, props);
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT, "test-description", parentNodeUrn);

    CreateGlossaryNodeResolver resolver = new CreateGlossaryNodeResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    verifyIngestProposal(mockClient, 1, proposal);
  }

  @Test
  public void testGetSuccessNoDescription() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT_NO_DESCRIPTION, "", parentNodeUrn);

    CreateGlossaryNodeResolver resolver = new CreateGlossaryNodeResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    verifyIngestProposal(mockClient, 1, proposal);
  }

  @Test
  public void testGetSuccessNoParentNode() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService<?> mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT_NO_PARENT_NODE, "test-description", null);

    CreateGlossaryNodeResolver resolver = new CreateGlossaryNodeResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    verifyIngestProposal(mockClient, 1, proposal);
  }
}
