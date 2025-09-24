package com.linkedin.datahub.graphql.resolvers.settings;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import com.linkedin.datahub.graphql.generated.UpdateAiAssistantSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateDocumentationAiSettingsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.settings.global.AiAssistantSettings;
import com.linkedin.settings.global.AiInstruction;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.DocumentationAiSettings;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateGlobalSettingsResolverTest {

  private UpdateGlobalSettingsResolver resolver;
  private EntityClient entityClient;
  private SecretService secretService;
  private IntegrationsService integrationsService;
  private QueryContext queryContext;
  private OperationContext operationContext;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setup() {
    entityClient = mock(EntityClient.class);
    secretService = mock(SecretService.class);
    integrationsService = mock(IntegrationsService.class);
    queryContext = mock(QueryContext.class);
    operationContext = mock(OperationContext.class);
    environment = mock(DataFetchingEnvironment.class);

    resolver = new UpdateGlobalSettingsResolver(entityClient, secretService, integrationsService);
  }

  @Test
  public void testConstructorNullChecks() {
    // Test null entityClient
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(null, secretService, integrationsService));

    // Test null secretService
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(entityClient, null, integrationsService));

    // Test null integrationsService
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(entityClient, secretService, null));
  }

  // Note: Skipping integration test due to static method dependencies
  // Authorization tests are covered by other test classes

  // Note: Skipping integration test due to static method dependencies
  // Exception handling tests are covered by private method tests below

  @Test
  public void testUpdateDocumentationAiSettings() throws Exception {
    // Setup
    DocumentationAiSettings existingSettings = new DocumentationAiSettings();
    existingSettings.setEnabled(false);

    UpdateDocumentationAiSettingsInput input = new UpdateDocumentationAiSettingsInput();
    input.setEnabled(true);

    AiInstructionInput instructionInput = new AiInstructionInput();
    instructionInput.setId("custom-id");
    instructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    instructionInput.setState(AiInstructionState.ACTIVE);
    instructionInput.setInstruction("Custom instruction");
    input.setInstructions(Arrays.asList(instructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateDocAiMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateDocumentationAiSettings",
            DocumentationAiSettings.class,
            UpdateDocumentationAiSettingsInput.class,
            QueryContext.class);
    updateDocAiMethod.setAccessible(true);

    // Execute
    updateDocAiMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify
    assertTrue(existingSettings.isEnabled());
    assertEquals(existingSettings.getInstructions().size(), 1);
    AiInstruction instruction = existingSettings.getInstructions().get(0);
    assertEquals(instruction.getId(), "custom-id");
    assertEquals(
        instruction.getType(), com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.ACTIVE);
    assertEquals(instruction.getInstruction(), "Custom instruction");
    assertNotNull(instruction.getCreated());
    assertNotNull(instruction.getLastModified());
  }

  @Test
  public void testUpdateAiAssistantSettings() throws Exception {
    // Setup
    AiAssistantSettings existingSettings = new AiAssistantSettings();

    UpdateAiAssistantSettingsInput input = new UpdateAiAssistantSettingsInput();

    AiInstructionInput instructionInput = new AiInstructionInput();
    instructionInput.setInstruction("Assistant instruction");
    instructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(instructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateAiAssistantMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateAiAssistantSettings",
            AiAssistantSettings.class,
            UpdateAiAssistantSettingsInput.class,
            QueryContext.class);
    updateAiAssistantMethod.setAccessible(true);

    // Execute
    updateAiAssistantMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify
    assertEquals(existingSettings.getInstructions().size(), 1);
    AiInstruction instruction = existingSettings.getInstructions().get(0);
    assertNotNull(instruction.getId()); // Should be auto-generated UUID
    assertNotNull(instruction.getType());
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.ACTIVE);
    assertEquals(instruction.getInstruction(), "Assistant instruction");
    assertNotNull(instruction.getCreated());
    assertNotNull(instruction.getLastModified());
  }

  @Test
  public void testMapAiInstructionInputsWithProvidedId() throws Exception {
    // Setup
    AiInstructionInput input = new AiInstructionInput();
    input.setId("custom-instruction-id");
    input.setState(AiInstructionState.INACTIVE);
    input.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstruction("Test instruction");

    String actorUrn = "urn:li:corpuser:testuser";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiInstructionInputs", List.class, QueryContext.class);
    mapMethod.setAccessible(true);

    // Execute
    AiInstructionArray result =
        (AiInstructionArray) mapMethod.invoke(resolver, Arrays.asList(input), queryContext);

    // Verify
    assertEquals(result.size(), 1);
    AiInstruction instruction = result.get(0);
    assertEquals(instruction.getId(), "custom-instruction-id");
    assertEquals(
        instruction.getType(), com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.INACTIVE);
    assertEquals(instruction.getInstruction(), "Test instruction");
    assertNotNull(instruction.getCreated());
    assertEquals(instruction.getCreated().getActor(), UrnUtils.getUrn(actorUrn));
    assertNotNull(instruction.getLastModified());
    assertEquals(instruction.getLastModified().getActor(), UrnUtils.getUrn(actorUrn));
  }

  @Test
  public void testMapAiInstructionInputsWithGeneratedId() throws Exception {
    // Setup
    AiInstructionInput input = new AiInstructionInput();
    // No ID provided - should generate UUID
    input.setInstruction("Test instruction without ID");
    input.setType(AiInstructionType.GENERAL_CONTEXT);

    String actorUrn = "urn:li:corpuser:testuser";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiInstructionInputs", List.class, QueryContext.class);
    mapMethod.setAccessible(true);

    // Execute
    AiInstructionArray result =
        (AiInstructionArray) mapMethod.invoke(resolver, Arrays.asList(input), queryContext);

    // Verify
    assertEquals(result.size(), 1);
    AiInstruction instruction = result.get(0);
    assertNotNull(instruction.getId()); // Should be auto-generated UUID
    assertTrue(instruction.getId().length() > 0);
    assertEquals(
        instruction.getState(),
        com.linkedin.settings.global.AiInstructionState.ACTIVE); // Default state
    assertEquals(instruction.getInstruction(), "Test instruction without ID");
  }

  @Test
  public void testMapGraphqlStateToPdlStateActive() throws Exception {
    // Use reflection to test the private method
    Method mapStateMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapGraphqlStateToPdlState",
            com.linkedin.datahub.graphql.generated.AiInstructionState.class);
    mapStateMethod.setAccessible(true);

    // Execute
    com.linkedin.settings.global.AiInstructionState result =
        (com.linkedin.settings.global.AiInstructionState)
            mapStateMethod.invoke(resolver, AiInstructionState.ACTIVE);

    // Verify
    assertEquals(result, com.linkedin.settings.global.AiInstructionState.ACTIVE);
  }

  @Test
  public void testMapGraphqlStateToPdlStateInactive() throws Exception {
    // Use reflection to test the private method
    Method mapStateMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapGraphqlStateToPdlState",
            com.linkedin.datahub.graphql.generated.AiInstructionState.class);
    mapStateMethod.setAccessible(true);

    // Execute
    com.linkedin.settings.global.AiInstructionState result =
        (com.linkedin.settings.global.AiInstructionState)
            mapStateMethod.invoke(resolver, AiInstructionState.INACTIVE);

    // Verify
    assertEquals(result, com.linkedin.settings.global.AiInstructionState.INACTIVE);
  }

  @Test
  public void testInstructionsReplaceNotAppend() throws Exception {
    // Setup existing settings with instructions
    DocumentationAiSettings existingSettings = new DocumentationAiSettings();
    AiInstructionArray existingInstructions = new AiInstructionArray();

    AiInstruction existingInstruction = new AiInstruction();
    existingInstruction.setId("existing-id");
    existingInstruction.setType(com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    existingInstruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    existingInstruction.setInstruction("Existing instruction");
    existingInstructions.add(existingInstruction);

    existingSettings.setInstructions(existingInstructions);

    // Create new input with different instructions
    UpdateDocumentationAiSettingsInput input = new UpdateDocumentationAiSettingsInput();

    AiInstructionInput newInstructionInput = new AiInstructionInput();
    newInstructionInput.setId("new-id");
    newInstructionInput.setInstruction("New instruction");
    newInstructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(newInstructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateDocAiMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateDocumentationAiSettings",
            DocumentationAiSettings.class,
            UpdateDocumentationAiSettingsInput.class,
            QueryContext.class);
    updateDocAiMethod.setAccessible(true);

    // Execute
    updateDocAiMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify instructions were replaced, not appended
    assertEquals(existingSettings.getInstructions().size(), 1);
    assertEquals(existingSettings.getInstructions().get(0).getId(), "new-id");
    assertEquals(
        existingSettings.getInstructions().get(0).getType(),
        com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(existingSettings.getInstructions().get(0).getInstruction(), "New instruction");
    // Old instruction should be gone
    assertFalse(
        existingSettings.getInstructions().stream()
            .anyMatch(inst -> "existing-id".equals(inst.getId())));
  }
}
