package com.linkedin.gms.factory.plugins;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.validation.*;
import com.linkedin.metadata.dataproducts.sideeffects.DataProductUnsetSideEffect;
import com.linkedin.metadata.entity.versioning.sideeffects.VersionPropertiesSideEffect;
import com.linkedin.metadata.entity.versioning.sideeffects.VersionSetSideEffect;
import com.linkedin.metadata.entity.versioning.validation.VersionPropertiesValidator;
import com.linkedin.metadata.entity.versioning.validation.VersionSetPropertiesValidator;
import com.linkedin.metadata.forms.validation.FormPromptValidator;
import com.linkedin.metadata.ingestion.validation.ExecuteIngestionAuthValidator;
import com.linkedin.metadata.ingestion.validation.ModifyIngestionSourceAuthValidator;
import com.linkedin.metadata.schemafields.sideeffects.SchemaFieldSideEffect;
import com.linkedin.metadata.structuredproperties.validation.HidePropertyValidator;
import com.linkedin.metadata.structuredproperties.validation.ShowPropertyAsBadgeValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = SpringStandardPluginConfiguration.class)
@TestPropertySource(
    properties = {
      "metadataChangeProposal.validation.ignoreUnknown=true",
      "metadataChangeProposal.validation.extensions.enabled=false",
      "metadataChangeProposal.sideEffects.schemaField.enabled=true",
      "metadataChangeProposal.sideEffects.dataProductUnset.enabled=true"
    })
public class StandardPluginConfigurationTest extends AbstractTestNGSpringContextTests {

  @Autowired private ApplicationContext context;

  @Test
  public void testIgnoreUnknownMutatorBeanCreation() {
    assertTrue(context.containsBean("ignoreUnknownMutator"));
    MutationHook mutator = context.getBean("ignoreUnknownMutator", MutationHook.class);
    assertNotNull(mutator);
    assertTrue(mutator instanceof IgnoreUnknownMutator);

    // Verify configuration
    assertNotNull(mutator.getConfig());
    assertTrue(mutator.getConfig().isEnabled());
    assertEquals(mutator.getConfig().getClassName(), IgnoreUnknownMutator.class.getName());
  }

  @Test
  public void testSchemaFieldSideEffectBeanCreation() {
    assertTrue(context.containsBean("schemaFieldSideEffect"));
    MCPSideEffect sideEffect = context.getBean("schemaFieldSideEffect", MCPSideEffect.class);
    assertNotNull(sideEffect);
    assertTrue(sideEffect instanceof SchemaFieldSideEffect);

    // Verify configuration
    assertNotNull(sideEffect.getConfig());
    assertTrue(sideEffect.getConfig().isEnabled());
    assertEquals(sideEffect.getConfig().getClassName(), SchemaFieldSideEffect.class.getName());
  }

  @Test
  public void testDataProductUnsetSideEffectBeanCreation() {
    assertTrue(context.containsBean("dataProductUnsetSideEffect"));
    MCPSideEffect sideEffect = context.getBean("dataProductUnsetSideEffect", MCPSideEffect.class);
    assertNotNull(sideEffect);
    assertTrue(sideEffect instanceof DataProductUnsetSideEffect);

    // Verify configuration
    assertNotNull(sideEffect.getConfig());
    assertTrue(sideEffect.getConfig().isEnabled());
    assertEquals(sideEffect.getConfig().getClassName(), DataProductUnsetSideEffect.class.getName());
  }

  @Test
  public void testFieldPathValidatorBeanCreation() {
    assertTrue(context.containsBean("fieldPathValidator"));
    AspectPayloadValidator validator =
        context.getBean("fieldPathValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof FieldPathValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(validator.getConfig().getClassName(), FieldPathValidator.class.getName());
  }

  @Test
  public void testDataHubExecutionRequestResultValidatorBeanCreation() {
    assertTrue(context.containsBean("dataHubExecutionRequestResultValidator"));
    AspectPayloadValidator validator =
        context.getBean("dataHubExecutionRequestResultValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof ExecutionRequestResultValidator);
  }

  @Test
  public void testHidePropertyValidatorBeanCreation() {
    assertTrue(context.containsBean("hidePropertyValidator"));
    AspectPayloadValidator validator =
        context.getBean("hidePropertyValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof HidePropertyValidator);
  }

  @Test
  public void testShowPropertyAsAssetBadgeValidatorBeanCreation() {
    assertTrue(context.containsBean("showPropertyAsAssetBadgeValidator"));
    AspectPayloadValidator validator =
        context.getBean("showPropertyAsAssetBadgeValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof ShowPropertyAsBadgeValidator);
  }

  @Test
  public void testFormPromptValidatorBeanCreation() {
    assertTrue(context.containsBean("formPromptValidator"));
    AspectPayloadValidator validator =
        context.getBean("formPromptValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof FormPromptValidator);
  }

  @Test
  public void testVersionPropertiesValidatorBeanCreation() {
    assertTrue(context.containsBean("versionPropertiesValidator"));
    AspectPayloadValidator validator =
        context.getBean("versionPropertiesValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof VersionPropertiesValidator);
  }

  @Test
  public void testVersionSetPropertiesValidatorBeanCreation() {
    assertTrue(context.containsBean("versionSetPropertiesValidator"));
    AspectPayloadValidator validator =
        context.getBean("versionSetPropertiesValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof VersionSetPropertiesValidator);
  }

  @Test
  public void testVersionPropertiesSideEffectBeanCreation() {
    assertTrue(context.containsBean("versionPropertiesSideEffect"));
    MCPSideEffect sideEffect = context.getBean("versionPropertiesSideEffect", MCPSideEffect.class);
    assertNotNull(sideEffect);
    assertTrue(sideEffect instanceof VersionPropertiesSideEffect);
  }

  @Test
  public void testVersionSetSideEffectBeanCreation() {
    assertTrue(context.containsBean("versionSetSideEffect"));
    MCPSideEffect sideEffect = context.getBean("versionSetSideEffect", MCPSideEffect.class);
    assertNotNull(sideEffect);
    assertTrue(sideEffect instanceof VersionSetSideEffect);
  }

  @Test
  public void testUrnAnnotationValidatorBeanCreation() {
    assertTrue(context.containsBean("urnAnnotationValidator"));
    AspectPayloadValidator validator =
        context.getBean("urnAnnotationValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof UrnAnnotationValidator);
  }

  @Test
  public void testUserDeleteValidatorBeanCreation() {
    assertTrue(context.containsBean("UserDeleteValidator"));
    AspectPayloadValidator validator =
        context.getBean("UserDeleteValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof UserDeleteValidator);
  }

  @Test
  public void testModifyIngestionSourceAuthValidatorBeanCreation() {
    assertTrue(context.containsBean("ModifyIngestionSourceAuthValidator"));
    AspectPayloadValidator validator =
        context.getBean("ModifyIngestionSourceAuthValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof ModifyIngestionSourceAuthValidator);
  }

  @Test
  public void testExecuteIngestionAuthValidatorBeanCreation() {
    assertTrue(context.containsBean("ExecuteIngestionAuthValidator"));
    AspectPayloadValidator validator =
        context.getBean("ExecuteIngestionAuthValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof ExecuteIngestionAuthValidator);
  }
}
