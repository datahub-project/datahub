package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.metadata.service.ActionRequestService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposeCreateGlossaryNodeResolverTest {
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:test";
  private static final String GLOSSARY_NODE_NAME = "GLOSSARY_NODE";

  private ActionRequestService _ActionRequestService;
  private ProposeCreateGlossaryNodeResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;
  private Authorizer _authorizer;

  @BeforeMethod
  public void setupTest() {
    _ActionRequestService = mock(ActionRequestService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);
    _authorizer = mock(Authorizer.class);

    _resolver = new ProposeCreateGlossaryNodeResolver(_ActionRequestService);
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
    input.setName(GLOSSARY_NODE_NAME);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_ActionRequestService.proposeCreateGlossaryNode(
            any(OperationContext.class), any(), eq(GLOSSARY_NODE_NAME), any(), any(), eq(null)))
        .thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
