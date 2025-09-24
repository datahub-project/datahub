package com.linkedin.datahub.graphql.resolvers.settings;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AiAssistantSettings;
import com.linkedin.datahub.graphql.generated.AiInstruction;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.DocumentationAiSettings;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.services.SecretService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SettingsMapperTest {

  private SettingsMapper settingsMapper;
  private FeatureFlags featureFlags;
  private SecretService secretService;
  private QueryContext queryContext;

  @BeforeMethod
  public void setup() {
    featureFlags = mock(FeatureFlags.class);
    secretService = mock(SecretService.class);
    queryContext = mock(QueryContext.class);
    settingsMapper = new SettingsMapper(secretService, featureFlags);
  }

  @Test
  public void testMapGlobalSettingsWithAiFeatures() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Assistant Settings
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("test-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    instruction.setInstruction("Test AI instruction");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    globalSettingsInfo.setAiAssistant(aiAssistantSettings);

    // Create Documentation AI Settings
    com.linkedin.settings.global.DocumentationAiSettings docAiSettings =
        new com.linkedin.settings.global.DocumentationAiSettings();
    docAiSettings.setEnabled(true);
    docAiSettings.setInstructions(instructions);
    globalSettingsInfo.setDocumentationAi(docAiSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiAssistant.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "test-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Test AI instruction");
    assertNotNull(mappedInstruction.getCreated());
    assertEquals(mappedInstruction.getCreated().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getCreated().getActor(), "urn:li:corpuser:testuser");
    assertNotNull(mappedInstruction.getLastModified());
    assertEquals(mappedInstruction.getLastModified().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getLastModified().getActor(), "urn:li:corpuser:testuser");

    // Verify Documentation AI Settings
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertTrue(mappedDocAi.getEnabled());
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 1);

    AiInstruction mappedDocInstruction = mappedDocAi.getInstructions().get(0);
    assertEquals(mappedDocInstruction.getId(), "test-instruction-id");
    assertEquals(mappedDocInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedDocInstruction.getInstruction(), "Test AI instruction");
    assertNotNull(mappedDocInstruction.getCreated());
    assertNotNull(mappedDocInstruction.getLastModified());
  }

  @Test
  public void testMapGlobalSettingsWithAiFeaturesDisabled() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(false);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings (should have empty instructions)
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 0);

    // Verify Documentation AI Settings (should be disabled)
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertFalse(mappedDocAi.getEnabled());
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithEmptyAiSettings() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(false);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings (should have empty instructions)
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 0);

    // Verify Documentation AI Settings (should use default enabled flag)
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertFalse(
        mappedDocAi.getEnabled()); // Should match featureFlags.isDocumentationAiDefaultEnabled()
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithInactiveInstructions() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Assistant Settings with INACTIVE instruction
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("inactive-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.INACTIVE);
    instruction.setInstruction("Inactive AI instruction");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    globalSettingsInfo.setAiAssistant(aiAssistantSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings with INACTIVE state
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiAssistant.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "inactive-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.INACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Inactive AI instruction");
    assertNotNull(mappedInstruction.getCreated());
    assertNotNull(mappedInstruction.getLastModified());
  }
}
