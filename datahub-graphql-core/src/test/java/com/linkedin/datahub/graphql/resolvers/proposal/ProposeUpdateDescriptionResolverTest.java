package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.service.ActionRequestService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposeUpdateDescriptionResolverTest {
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:test";
  private static final String UNSUPPORTED_ENTITY_URN_STRING = "urn:li:chart:(looker,baz1)";

  private static final String GLOSSARY_NODE_URN_STRING =
      "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";
  private static final String GLOSSARY_TERM_URN_STRING =
      "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";
  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)";
  private static final String DESCRIPTION = "description";

  private ActionRequestService _ActionRequestService;
  private ProposeUpdateDescriptionResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() {
    _ActionRequestService = mock(ActionRequestService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new ProposeUpdateDescriptionResolver(_ActionRequestService);
  }

  @Test
  public void testFailsUnsupportedEntityType() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);

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

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(GLOSSARY_NODE_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_ActionRequestService.proposeUpdateResourceDescription(
            any(OperationContext.class), any(), any(), any(), any(), any(), eq(null)))
        .thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesGlossaryTerm() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(GLOSSARY_TERM_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_ActionRequestService.proposeUpdateResourceDescription(
            any(OperationContext.class), any(), any(), any(), any(), any(), eq(null)))
        .thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesDataset() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(DATASET_URN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_ActionRequestService.proposeUpdateResourceDescription(
            any(OperationContext.class), any(), any(), any(), any(), any(), eq(null)))
        .thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testPassesColumn() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getActorUrn()).thenReturn(ACTOR_URN_STRING);

    String fieldPath = "someField";

    DescriptionUpdateInput input = new DescriptionUpdateInput();
    input.setDescription(DESCRIPTION);
    input.setResourceUrn(DATASET_URN_STRING);
    input.setSubResourceType(SubResourceType.DATASET_FIELD);
    input.setSubResource(fieldPath);

    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);
    when(_ActionRequestService.proposeUpdateResourceDescription(
            any(OperationContext.class),
            eq(Urn.createFromString(ACTOR_URN_STRING)),
            eq(Urn.createFromString(DATASET_URN_STRING)),
            eq(SubResourceType.DATASET_FIELD.toString()),
            eq(fieldPath),
            eq(DESCRIPTION),
            eq(null)))
        .thenReturn(true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }
}
