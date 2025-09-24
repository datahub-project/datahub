package com.linkedin.datahub.graphql.types.corpuser.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.AiAssistantSettings;
import com.linkedin.datahub.graphql.generated.AiInstruction;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import org.testng.annotations.Test;

public class CorpUserMapperTest {

  @Test
  public void testMapCorpUserWithAiAssistantSettings() {
    // Create entity response with corp user settings containing AI assistant settings
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI assistant settings
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Create AI assistant settings
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("user-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    instruction.setInstruction("Test AI instruction for user");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    corpUserSettings.setAiAssistant(aiAssistantSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify AI assistant settings are mapped correctly
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiAssistant());

    AiAssistantSettings mappedAiSettings = result.getSettings().getAiAssistant();
    assertNotNull(mappedAiSettings.getInstructions());
    assertEquals(mappedAiSettings.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiSettings.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "user-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Test AI instruction for user");
    assertNotNull(mappedInstruction.getCreated());
    assertEquals(mappedInstruction.getCreated().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getCreated().getActor(), "urn:li:corpuser:testUser");
    assertNotNull(mappedInstruction.getLastModified());
    assertEquals(mappedInstruction.getLastModified().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getLastModified().getActor(), "urn:li:corpuser:testUser");
  }

  @Test
  public void testMapCorpUserWithoutAiAssistantSettings() {
    // Create entity response with corp user settings but no AI assistant settings
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings without AI assistant settings
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify settings exist but AI assistant settings are null (not set)
    assertNotNull(result.getSettings());
    assertNull(result.getSettings().getAiAssistant());
  }

  @Test
  public void testMapCorpUserWithEmptyAiInstructions() {
    // Create entity response with corp user settings containing AI assistant settings with empty
    // instructions
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI assistant settings but empty instructions
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Create AI assistant settings with empty instructions
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    aiAssistantSettings.setInstructions(instructions);
    corpUserSettings.setAiAssistant(aiAssistantSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify AI assistant settings are mapped with empty instructions
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiAssistant());

    AiAssistantSettings mappedAiSettings = result.getSettings().getAiAssistant();
    assertNotNull(mappedAiSettings.getInstructions());
    assertEquals(mappedAiSettings.getInstructions().size(), 0);
  }
}
