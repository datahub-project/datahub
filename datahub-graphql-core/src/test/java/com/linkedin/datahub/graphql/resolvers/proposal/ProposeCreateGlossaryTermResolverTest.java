package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.proposal.ProposalService;
import com.datahub.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ProposeCreateGlossaryTermResolverTest {
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:test";
  private static final String GLOSSARY_TERM_NAME = "GLOSSARY_TERM";

  private ProposalService _proposalService;
  private ProposeCreateGlossaryTermResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private Authorizer _authorizer;

  @BeforeMethod
  public void setupTest() {
    _proposalService = mock(ProposalService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    _authorizer = mock(Authorizer.class);

    _resolver = new ProposeCreateGlossaryTermResolver(_proposalService);
  }

  @Test
  public void testFailsNullName() {
    CreateGlossaryEntityInput input = new CreateGlossaryEntityInput();
    input.setName(null);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsEmptyName() {
    CreateGlossaryEntityInput input = new CreateGlossaryEntityInput();
    input.setName("");
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);
    when(mockContext.getAuthorizer()).thenReturn(_authorizer);

    CreateGlossaryEntityInput input = new CreateGlossaryEntityInput();
    input.setName(GLOSSARY_TERM_NAME);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_proposalService.proposeCreateGlossaryTerm(any(), eq(GLOSSARY_TERM_NAME), any(), eq(_authorizer))).thenReturn(
        true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
