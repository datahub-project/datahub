package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.proposal.ProposalService;
import com.datahub.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ProposeUpdateDescriptionResolverTest {
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:test";
  private static final String UNSUPPORTED_ENTITY_URN_STRING = "urn:li:chart:(looker,baz1)";

  private static final String GLOSSARY_NODE_URN_STRING = "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";
  private static final String GLOSSARY_TERM_URN_STRING = "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";
  private static final String DESCRIPTION = "description";

  private ProposalService _proposalService;
  private ProposeUpdateDescriptionResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private Authorizer _authorizer;

  @BeforeMethod
  public void setupTest() {
    _proposalService = mock(ProposalService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    _authorizer = mock(Authorizer.class);

    _resolver = new ProposeUpdateDescriptionResolver(_proposalService);
  }

  @Test
  public void testFailsNotNullSubresource() {
    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setSubResource("subresource");
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsEmptyName() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);
    when(mockContext.getAuthorizer()).thenReturn(_authorizer);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setResourceUrn(UNSUPPORTED_ENTITY_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesGlossaryNode() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);
    when(mockContext.getAuthorizer()).thenReturn(_authorizer);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(GLOSSARY_NODE_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_proposalService.proposeUpdateResourceDescription(any(), any(), any(), eq(_authorizer))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesGlossaryTerm() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);
    when(mockContext.getAuthorizer()).thenReturn(_authorizer);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(GLOSSARY_TERM_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_proposalService.proposeUpdateResourceDescription(any(), any(), any(), eq(_authorizer))).thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
