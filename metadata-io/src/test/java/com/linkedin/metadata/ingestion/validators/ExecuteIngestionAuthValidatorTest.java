package com.linkedin.metadata.ingestion.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.ingestion.validation.ExecuteIngestionAuthValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ExecuteIngestionAuthValidatorTest {

  private static final Urn EXECUTION_REQUEST_URN;
  private static final Urn INGESTION_SOURCE_URN;
  private static final String INGESTION_SOURCE_URN_STRING = "urn:li:dataHubIngestionSource:test";
  private static final String EXECUTION_REQUEST_URN_STRING = "urn:li:dataHubExecutionRequest:test";

  private ExecuteIngestionAuthValidator validator;

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AuthorizationSession mockSession = Mockito.mock(AuthorizationSession.class);

  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new ExecuteIngestionAuthValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(ExecuteIngestionAuthValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "PATCH", "UPSERT", "CREATE_ENTITY"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                      .aspectName(EXECUTION_REQUEST_INPUT_ASPECT_NAME)
                      .build()))
          .build();

  static {
    try {
      EXECUTION_REQUEST_URN = Urn.createFromString(EXECUTION_REQUEST_URN_STRING);
      INGESTION_SOURCE_URN = Urn.createFromString(INGESTION_SOURCE_URN_STRING);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAllowed() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "EXECUTE_ENTITY");
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput();
    executionRequestInput.setSource(
        new ExecutionRequestSource().setIngestionSource(INGESTION_SOURCE_URN));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(EXECUTION_REQUEST_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                                .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
                        .recordTemplate(executionRequestInput)
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(EXECUTION_REQUEST_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                                .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
                        .recordTemplate(executionRequestInput)
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        0,
        "Expected Execution Request to be allowed when the user has execute permission on the Ingestion source");
  }

  @Test
  public void testDenied() {
    Set<String> allowedPrivileges = Set.of("MANAGE_INGESTION", "EXECUTE_ENTITY");
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput();
    executionRequestInput.setSource(
        new ExecutionRequestSource().setIngestionSource(INGESTION_SOURCE_URN));

    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(mockSession.authorize(
            argThat(allowedPrivileges::contains),
            argThat(
                entity ->
                    entity.equals(
                        new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING))),
            anyCollection()))
        .thenReturn(result);

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(EXECUTION_REQUEST_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                                .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
                        .recordTemplate(executionRequestInput)
                        .build(),
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(EXECUTION_REQUEST_URN)
                        .entitySpec(
                            entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                                .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
                        .recordTemplate(executionRequestInput)
                        .build()),
                mockRetrieverContext,
                mockSession)
            .count(),
        2,
        "Expected Execution Request to be denied when the user doesn't have the execute permission on the Ingestion source");
  }

  @Test
  public void testNullSession() {
    // Test when session is null
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput();
    executionRequestInput.setSource(
        new ExecutionRequestSource().setIngestionSource(INGESTION_SOURCE_URN));

    BatchItem testItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(EXECUTION_REQUEST_URN)
            .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                    .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
            .recordTemplate(executionRequestInput)
            .build();

    assertEquals(
        validator.validateProposed(Set.of(testItem), mockRetrieverContext, null).count(),
        0,
        "Expected no exceptions when session is null");
  }

  @Test
  public void testNullIngestionSourceUrn() {
    // Test when ingestion source URN is null
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput();
    executionRequestInput.setSource(new ExecutionRequestSource()); // No ingestion source set

    BatchItem testItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(EXECUTION_REQUEST_URN)
            .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                    .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
            .recordTemplate(executionRequestInput)
            .build();

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(Set.of(testItem), mockRetrieverContext, mockSession)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1);
    assertTrue(
        exceptions.get(0).getMessage().contains("Couldn't find the ingestion source details"));
    assertTrue(exceptions.get(0).getMessage().contains(EXECUTION_REQUEST_URN_STRING));
  }

  @Test
  public void testNullExecutionRequestInput() {
    // Test when ExecutionRequestInput is null (no aspect of the correct type)
    BatchItem testItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(EXECUTION_REQUEST_URN)
            .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                    .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
            .recordTemplate(null)
            .build();

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(Set.of(testItem), mockRetrieverContext, mockSession)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1);
    assertTrue(
        exceptions.get(0).getMessage().contains("Couldn't find the ingestion source details"));
  }

  @Test
  public void testEmptyBatchItems() {
    // Test with empty collection of batch items
    assertEquals(
        validator
            .validateProposed(Collections.emptySet(), mockRetrieverContext, mockSession)
            .count(),
        0,
        "Expected no exceptions for empty batch items");
  }

  @Test
  public void testMixedPermissions() {
    // Test with multiple items, some allowed and some denied
    final ExecutionRequestInput allowedInput = new ExecutionRequestInput();
    allowedInput.setSource(new ExecutionRequestSource().setIngestionSource(INGESTION_SOURCE_URN));

    final ExecutionRequestInput deniedInput = new ExecutionRequestInput();
    Urn deniedIngestionSourceUrn;
    try {
      deniedIngestionSourceUrn = Urn.createFromString("urn:li:dataHubIngestionSource:denied");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    deniedInput.setSource(
        new ExecutionRequestSource().setIngestionSource(deniedIngestionSourceUrn));

    // Setup authorization results
    AuthorizationResult allowResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(allowResult.getType()).thenReturn(AuthorizationResult.Type.ALLOW);

    AuthorizationResult denyResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);

    when(mockSession.authorize(
            any(String.class),
            eq(new EntitySpec("dataHubIngestionSource", INGESTION_SOURCE_URN_STRING)),
            anyCollection()))
        .thenReturn(allowResult);

    when(mockSession.authorize(
            any(String.class),
            eq(new EntitySpec("dataHubIngestionSource", "urn:li:dataHubIngestionSource:denied")),
            anyCollection()))
        .thenReturn(denyResult);

    BatchItem allowedItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(EXECUTION_REQUEST_URN)
            .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                    .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
            .recordTemplate(allowedInput)
            .build();

    BatchItem deniedItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(EXECUTION_REQUEST_URN)
            .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                    .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
            .recordTemplate(deniedInput)
            .build();

    List<AspectValidationException> exceptions =
        validator
            .validateProposed(Set.of(allowedItem, deniedItem), mockRetrieverContext, mockSession)
            .collect(Collectors.toList());

    assertEquals(exceptions.size(), 1, "Expected only one denied item");
    assertTrue(
        exceptions
            .get(0)
            .getMessage()
            .contains("Doesn't have permissions to execute this Ingestion"));
  }

  @Test
  public void testMultipleItemsAllDenied() {
    // Test with multiple items that are all denied
    final ExecutionRequestInput input1 = new ExecutionRequestInput();
    final ExecutionRequestInput input2 = new ExecutionRequestInput();

    try {
      Urn deniedUrn1 = Urn.createFromString("urn:li:dataHubIngestionSource:denied1");
      Urn deniedUrn2 = Urn.createFromString("urn:li:dataHubIngestionSource:denied2");

      input1.setSource(new ExecutionRequestSource().setIngestionSource(deniedUrn1));
      input2.setSource(new ExecutionRequestSource().setIngestionSource(deniedUrn2));

      AuthorizationResult denyResult = Mockito.mock(AuthorizationResult.class);
      Mockito.when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);

      when(mockSession.authorize(any(String.class), any(EntitySpec.class), anyCollection()))
          .thenReturn(denyResult);

      BatchItem item1 =
          TestMCP.builder()
              .changeType(ChangeType.UPSERT)
              .urn(EXECUTION_REQUEST_URN)
              .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_URN.getEntityType()))
              .aspectSpec(
                  entityRegistry
                      .getEntitySpec(EXECUTION_REQUEST_URN.getEntityType())
                      .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
              .recordTemplate(input1)
              .build();

      BatchItem item2 =
          TestMCP.builder()
              .changeType(ChangeType.CREATE)
              .urn(Urn.createFromString("urn:li:dataHubExecutionRequest:test2"))
              .entitySpec(entityRegistry.getEntitySpec(EXECUTION_REQUEST_ENTITY_NAME))
              .aspectSpec(
                  entityRegistry
                      .getEntitySpec(EXECUTION_REQUEST_ENTITY_NAME)
                      .getAspectSpec(EXECUTION_REQUEST_INPUT_ASPECT_NAME))
              .recordTemplate(input2)
              .build();

      List<AspectValidationException> exceptions =
          validator
              .validateProposed(Set.of(item1, item2), mockRetrieverContext, mockSession)
              .collect(Collectors.toList());

      assertEquals(exceptions.size(), 2, "Expected both items to be denied");
      assertTrue(
          exceptions.stream()
              .allMatch(
                  e ->
                      e.getMessage()
                          .contains("Doesn't have permissions to execute this Ingestion")));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConfigGetterSetter() {
    // Test config getter and setter
    AspectPluginConfig newConfig =
        AspectPluginConfig.builder()
            .className("TestClass")
            .enabled(false)
            .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
            .build();

    validator.setConfig(newConfig);
    assertEquals(validator.getConfig(), newConfig);

    // Test chaining
    ExecuteIngestionAuthValidator chainedValidator =
        new ExecuteIngestionAuthValidator().setConfig(TEST_PLUGIN_CONFIG);
    assertEquals(chainedValidator.getConfig(), TEST_PLUGIN_CONFIG);
  }
}
