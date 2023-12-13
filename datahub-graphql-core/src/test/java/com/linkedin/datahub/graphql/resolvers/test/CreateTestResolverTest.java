package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.TestUtils.*;
<<<<<<< HEAD
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
=======
import static org.testng.Assert.*;

>>>>>>> oss_master
import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateTestInput;
import com.linkedin.datahub.graphql.generated.TestDefinitionInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateTestResolverTest {
  private static final CreateTestInput TEST_INPUT =
      new CreateTestInput(
          "test-id",
          "test-name",
          "test-category",
          "test-description",
          new TestDefinitionInput("{}"));

<<<<<<< HEAD
  private EntityClient mockClient;
  private TestEngine mockEngine;
  private CreateTestResolver resolver;
  private DataFetchingEnvironment mockEnv;
  private Authentication authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    mockClient = mock(EntityClient.class);
    mockEngine = mock(TestEngine.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    authentication = mock(Authentication.class);
    resolver = new CreateTestResolver(mockClient, mockEngine);
  }
=======
  private static final CreateTestInput TEST_INPUT =
      new CreateTestInput(
          "test-id",
          "test-name",
          "test-category",
          "test-description",
          new TestDefinitionInput("{}"));
>>>>>>> oss_master

  @Test
  public void testGetSuccess() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEngine.validateJson(anyString())).thenReturn(ValidationResult.validResult());
    Mockito.when(mockContext.getAuthentication()).thenReturn(authentication);
    Mockito.when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "test-user"));

    resolver.get(mockEnv).get();

    final TestKey key = new TestKey();
    key.setId("test-id");

<<<<<<< HEAD
    // Not ideal to match against "any", but we don't know the auto-generated execution request id
=======
>>>>>>> oss_master
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            proposalCaptor.capture(), Mockito.any(Authentication.class), Mockito.eq(false));
    MetadataChangeProposal resultProposal = proposalCaptor.getValue();
    assertEquals(resultProposal.getEntityType(), Constants.TEST_ENTITY_NAME);
    assertEquals(resultProposal.getAspectName(), Constants.TEST_INFO_ASPECT_NAME);
    assertEquals(resultProposal.getChangeType(), ChangeType.UPSERT);
    assertEquals(resultProposal.getEntityKeyAspect(), GenericRecordUtils.serializeAspect(key));
    TestInfo resultInfo =
        GenericRecordUtils.deserializeAspect(
            resultProposal.getAspect().getValue(),
            resultProposal.getAspect().getContentType(),
            TestInfo.class);
    assertEquals(resultInfo.getName(), "test-name");
    assertEquals(resultInfo.getCategory(), "test-category");
    assertEquals(resultInfo.getDescription(), "test-description");
    assertEquals(resultInfo.getDefinition().getType(), TestDefinitionType.JSON);
    assertEquals(resultInfo.getDefinition().getJson(), "{}");
  }

  @Test
  public void testInvalidInput() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEngine.validateJson(anyString()))
        .thenReturn(new ValidationResult(false, Collections.emptyList()));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class), Mockito.eq(false));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Execute resolver
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
<<<<<<< HEAD
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class), Mockito.eq(false));
=======
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class));
>>>>>>> oss_master
  }

  @Test
  public void testGetEntityClientException() throws Exception {
<<<<<<< HEAD
=======
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class), Mockito.eq(false));
    CreateTestResolver resolver = new CreateTestResolver(mockClient);

>>>>>>> oss_master
    // Execute resolver
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class), Mockito.eq(false));
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
