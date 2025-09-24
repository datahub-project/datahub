package com.linkedin.datahub.graphql.resolvers.settings.user;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserAiAssistantSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.AiAssistantSettings;
import com.linkedin.settings.global.AiInstructionArray;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateCorpUserAiAssistantSettingsResolverTest {

  private UpdateCorpUserAiAssistantSettingsResolver resolver;
  private SettingsService settingsService;
  private QueryContext queryContext;
  private OperationContext operationContext;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setup() {
    settingsService = mock(SettingsService.class);
    queryContext = mock(QueryContext.class);
    operationContext = mock(OperationContext.class);
    environment = mock(DataFetchingEnvironment.class);
    resolver = new UpdateCorpUserAiAssistantSettingsResolver(settingsService);
  }

  @Test
  public void testSuccessfulUpdate() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction1 = new AiInstructionInput();
    instruction1.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction1.setInstruction("Test instruction 1");
    input.setInstructions(Arrays.asList(instruction1));

    Map<String, Object> arguments = new HashMap<>();
    arguments.put("input", input);
    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing settings
    CorpUserSettings existingSettings = new CorpUserSettings();
    existingSettings.setAppearance(
        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(existingSettings);

    // Mock successful update
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());
    verify(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));
  }

  @Test
  public void testUpdateWithNullExistingSettings() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setInstruction("Test instruction");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock null existing settings (new user)
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(null);

    // Mock successful update
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());
    verify(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));
  }

  @Test
  public void testUpdateWithExistingAiSettings() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setInstruction("Updated instruction");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing settings with AI assistant settings
    CorpUserSettings existingSettings = new CorpUserSettings();
    existingSettings.setAppearance(
        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));

    AiAssistantSettings existingAiSettings = new AiAssistantSettings();
    existingSettings.setAiAssistant(existingAiSettings);

    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(existingSettings);

    // Mock successful update
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());
    verify(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));
  }

  @Test
  public void testUpdateFailure() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    when(environment.getArgument("input")).thenReturn(input);

    // Mock settings service throws exception
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify exception is thrown
    assertThrows(ExecutionException.class, () -> result.get());
  }

  @Test
  public void testUuidGenerationWhenNoIdProvided() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input without ID - should generate UUID
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setInstruction("Test instruction");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    // No ID provided - should auto-generate
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock null existing settings
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(null);

    // Create argument captor to verify generated settings
    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), settingsCaptor.capture());

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());

    // Verify captured settings have generated UUID and correct state
    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    assertNotNull(capturedSettings.getAiAssistant());
    AiInstructionArray capturedInstructions = capturedSettings.getAiAssistant().getInstructions();
    assertEquals(capturedInstructions.size(), 1);

    com.linkedin.settings.global.AiInstruction capturedInstruction = capturedInstructions.get(0);
    assertNotNull(capturedInstruction.getId()); // UUID should be generated
    assertEquals(
        capturedInstruction.getState(), com.linkedin.settings.global.AiInstructionState.ACTIVE);
    assertEquals(
        capturedInstruction.getType(),
        com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(capturedInstruction.getInstruction(), "Test instruction");
    assertNotNull(capturedInstruction.getCreated());
    assertNotNull(capturedInstruction.getLastModified());
  }

  @Test
  public void testProvidedIdAndStateAreUsed() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input with provided ID and state
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setId("custom-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(AiInstructionState.INACTIVE);
    instruction.setInstruction("Test instruction with custom ID and state");
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing settings
    CorpUserSettings existingSettings = new CorpUserSettings();
    existingSettings.setAppearance(
        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(existingSettings);

    // Create argument captor to verify provided values are used
    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), settingsCaptor.capture());

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());

    // Verify captured settings use provided ID and state
    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    assertNotNull(capturedSettings.getAiAssistant());
    AiInstructionArray capturedInstructions = capturedSettings.getAiAssistant().getInstructions();
    assertEquals(capturedInstructions.size(), 1);

    com.linkedin.settings.global.AiInstruction capturedInstruction = capturedInstructions.get(0);
    assertEquals(capturedInstruction.getId(), "custom-instruction-id"); // Should use provided ID
    assertEquals(
        capturedInstruction.getType(),
        com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(
        capturedInstruction.getState(),
        com.linkedin.settings.global.AiInstructionState.INACTIVE); // Should use provided state
    assertEquals(capturedInstruction.getInstruction(), "Test instruction with custom ID and state");
    assertNotNull(capturedInstruction.getCreated());
    assertNotNull(capturedInstruction.getLastModified());
  }

  @Test
  public void testDefaultStateWhenNotProvided() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input with ID but no state - should default to ACTIVE
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setId("custom-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    // No state provided - should default to ACTIVE
    instruction.setInstruction("Test instruction with default state");
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing settings
    CorpUserSettings existingSettings = new CorpUserSettings();
    existingSettings.setAppearance(
        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(existingSettings);

    // Create argument captor to verify default state
    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), settingsCaptor.capture());

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify
    assertTrue(result.get());

    // Verify captured settings default to ACTIVE state
    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    assertNotNull(capturedSettings.getAiAssistant());
    AiInstructionArray capturedInstructions = capturedSettings.getAiAssistant().getInstructions();
    assertEquals(capturedInstructions.size(), 1);

    com.linkedin.settings.global.AiInstruction capturedInstruction = capturedInstructions.get(0);
    assertEquals(capturedInstruction.getId(), "custom-instruction-id");
    assertEquals(
        capturedInstruction.getType(),
        com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(
        capturedInstruction.getState(),
        com.linkedin.settings.global.AiInstructionState.ACTIVE); // Should default to ACTIVE
    assertEquals(capturedInstruction.getInstruction(), "Test instruction with default state");
  }

  @Test
  public void testValidationPassesForValidInstructions() throws Exception {
    // Setup
    String userUrn = "urn:li:corpuser:testUser";
    when(queryContext.getActorUrn()).thenReturn(userUrn);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
    when(environment.getContext()).thenReturn(queryContext);

    // Create input with valid instructions (single GENERAL_CONTEXT and no duplicate IDs)
    UpdateCorpUserAiAssistantSettingsInput input = new UpdateCorpUserAiAssistantSettingsInput();
    AiInstructionInput instruction = new AiInstructionInput();
    instruction.setId("unique-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setInstruction("Valid instruction");
    input.setInstructions(Arrays.asList(instruction));

    when(environment.getArgument("input")).thenReturn(input);

    // Mock existing settings
    CorpUserSettings existingSettings = new CorpUserSettings();
    existingSettings.setAppearance(
        new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
    when(settingsService.getCorpUserSettings(eq(operationContext), eq(UrnUtils.getUrn(userUrn))))
        .thenReturn(existingSettings);

    // Mock successful update
    doNothing()
        .when(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));

    // Execute
    CompletableFuture<Boolean> result = resolver.get(environment);

    // Verify success
    assertTrue(result.get());
    verify(settingsService)
        .updateCorpUserSettings(
            eq(operationContext), eq(UrnUtils.getUrn(userUrn)), any(CorpUserSettings.class));
  }
}
