package com.linkedin.gms.factory.plugins;

import static org.testng.Assert.*;

import com.linkedin.metadata.aspect.hooks.FieldPathMutator;
import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.hooks.OwnershipOwnerTypes;
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
import com.linkedin.metadata.structuredproperties.hooks.PropertyDefinitionDeleteSideEffect;
import com.linkedin.metadata.structuredproperties.hooks.StructuredPropertiesSoftDelete;
import com.linkedin.metadata.structuredproperties.validation.HidePropertyValidator;
import com.linkedin.metadata.structuredproperties.validation.PropertyDefinitionValidator;
import com.linkedin.metadata.structuredproperties.validation.ShowPropertyAsBadgeValidator;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertiesValidator;
import java.util.List;
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

  @Test
  public void testCreateIfNotExistsValidatorBeanCreation() {
    assertTrue(context.containsBean("createIfNotExistsValidator"));
    AspectPayloadValidator validator =
        context.getBean("createIfNotExistsValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof CreateIfNotExistsValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(validator.getConfig().getClassName(), CreateIfNotExistsValidator.class.getName());
    assertEquals(
        validator.getConfig().getSupportedOperations(), List.of("CREATE", "CREATE_ENTITY"));
  }

  @Test
  public void testConditionalWriteValidatorBeanCreation() {
    assertTrue(context.containsBean("conditionalWriteValidator"));
    AspectPayloadValidator validator =
        context.getBean("conditionalWriteValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof ConditionalWriteValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(validator.getConfig().getClassName(), ConditionalWriteValidator.class.getName());
    assertEquals(
        validator.getConfig().getSupportedOperations(),
        List.of("CREATE", "CREATE_ENTITY", "DELETE", "UPSERT", "UPDATE", "PATCH"));
  }

  @Test
  public void testFieldPathMutatorBeanCreation() {
    assertTrue(context.containsBean("fieldPathMutator"));
    MutationHook mutator = context.getBean("fieldPathMutator", MutationHook.class);
    assertNotNull(mutator);
    assertTrue(mutator instanceof FieldPathMutator);

    // Verify configuration
    assertNotNull(mutator.getConfig());
    assertTrue(mutator.getConfig().isEnabled());
    assertEquals(mutator.getConfig().getClassName(), FieldPathMutator.class.getName());
    assertEquals(
        mutator.getConfig().getSupportedOperations(),
        List.of("CREATE", "UPSERT", "UPDATE", "RESTATE", "PATCH"));
  }

  @Test
  public void testOwnershipOwnerTypesBeanCreation() {
    assertTrue(context.containsBean("ownershipOwnerTypes"));
    MutationHook mutator = context.getBean("ownershipOwnerTypes", MutationHook.class);
    assertNotNull(mutator);
    assertTrue(mutator instanceof OwnershipOwnerTypes);

    // Verify configuration
    assertNotNull(mutator.getConfig());
    assertTrue(mutator.getConfig().isEnabled());
    assertEquals(mutator.getConfig().getClassName(), OwnershipOwnerTypes.class.getName());
    assertEquals(
        mutator.getConfig().getSupportedOperations(),
        List.of("CREATE", "UPSERT", "UPDATE", "RESTATE", "PATCH"));
  }

  @Test
  public void testStructuredPropertiesSoftDeleteBeanCreation() {
    assertTrue(context.containsBean("structuredPropertiesSoftDelete"));
    MutationHook mutator = context.getBean("structuredPropertiesSoftDelete", MutationHook.class);
    assertNotNull(mutator);
    assertTrue(mutator instanceof StructuredPropertiesSoftDelete);

    // Verify configuration
    assertNotNull(mutator.getConfig());
    assertTrue(mutator.getConfig().isEnabled());
    assertEquals(
        mutator.getConfig().getClassName(), StructuredPropertiesSoftDelete.class.getName());
    // Note: This plugin doesn't define supportedOperations, only supportedEntityAspectNames
  }

  @Test
  public void testPropertyDefinitionValidatorBeanCreation() {
    assertTrue(context.containsBean("propertyDefinitionValidator"));
    AspectPayloadValidator validator =
        context.getBean("propertyDefinitionValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof PropertyDefinitionValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(validator.getConfig().getClassName(), PropertyDefinitionValidator.class.getName());
    assertEquals(
        validator.getConfig().getSupportedOperations(),
        List.of("CREATE", "CREATE_ENTITY", "UPSERT"));
  }

  @Test
  public void testStructuredPropertiesValidatorBeanCreation() {
    assertTrue(context.containsBean("structuredPropertiesValidator"));
    AspectPayloadValidator validator =
        context.getBean("structuredPropertiesValidator", AspectPayloadValidator.class);
    assertNotNull(validator);
    assertTrue(validator instanceof StructuredPropertiesValidator);

    // Verify configuration
    assertNotNull(validator.getConfig());
    assertTrue(validator.getConfig().isEnabled());
    assertEquals(
        validator.getConfig().getClassName(), StructuredPropertiesValidator.class.getName());
    assertEquals(
        validator.getConfig().getSupportedOperations(), List.of("CREATE", "UPSERT", "DELETE"));
  }

  @Test
  public void testPropertyDefinitionDeleteSideEffectBeanCreation() {
    assertTrue(context.containsBean("propertyDefinitionDeleteSideEffect"));
    MCPSideEffect sideEffect =
        context.getBean("propertyDefinitionDeleteSideEffect", MCPSideEffect.class);
    assertNotNull(sideEffect);
    assertTrue(sideEffect instanceof PropertyDefinitionDeleteSideEffect);

    // Verify configuration
    assertNotNull(sideEffect.getConfig());
    assertTrue(sideEffect.getConfig().isEnabled());
    assertEquals(
        sideEffect.getConfig().getClassName(), PropertyDefinitionDeleteSideEffect.class.getName());
    assertEquals(sideEffect.getConfig().getSupportedOperations(), List.of("DELETE"));
  }
}
