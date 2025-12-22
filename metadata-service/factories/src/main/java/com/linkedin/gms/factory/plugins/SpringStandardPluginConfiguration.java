package com.linkedin.gms.factory.plugins;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectSerializationHook;
import com.linkedin.metadata.aspect.hooks.FieldPathMutator;
import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.hooks.OwnershipOwnerTypes;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.validation.ConditionalWriteValidator;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.aspect.validation.ExecutionRequestResultValidator;
import com.linkedin.metadata.aspect.validation.FieldPathValidator;
import com.linkedin.metadata.aspect.validation.PolicyFieldTypeValidator;
import com.linkedin.metadata.aspect.validation.PrivilegeConstraintsValidator;
import com.linkedin.metadata.aspect.validation.SystemPolicyValidator;
import com.linkedin.metadata.aspect.validation.UrnAnnotationValidator;
import com.linkedin.metadata.aspect.validation.UserDeleteValidator;
import com.linkedin.metadata.config.AspectSizeValidationConfig;
import com.linkedin.metadata.config.PoliciesConfiguration;
import com.linkedin.metadata.dataproducts.sideeffects.DataProductUnsetSideEffect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectSizeValidationHook;
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
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.SchemaMetadataChangeEventGenerator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SpringStandardPluginConfiguration {
  private static final String ALL = "*";
  private static final String UPSERT = "UPSERT";
  private static final String UPDATE = "UPDATE";
  private static final String CREATE = "CREATE";
  private static final String CREATE_ENTITY = "CREATE_ENTITY";
  private static final String PATCH = "PATCH";
  private static final String DELETE = "DELETE";
  private static final String RESTATE = "RESTATE";
  // Authentication validation should consider all change types as best practice
  private static final List<String> AUTH_CHANGE_TYPE_OPERATIONS =
      List.of(CREATE, CREATE_ENTITY, UPDATE, UPSERT, PATCH, DELETE, RESTATE);

  @Value("${metadataChangeProposal.validation.ignoreUnknown}")
  private boolean ignoreUnknownEnabled;

  @Value("${metadataChangeProposal.validation.extensions.enabled:false}")
  private boolean extensionsEnabled;

  @Bean
  @ConditionalOnProperty(
      name = "metadataChangeProposal.validation.extensions.enabled",
      havingValue = "false")
  public MutationHook ignoreUnknownMutator() {
    return new IgnoreUnknownMutator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(IgnoreUnknownMutator.class.getName())
                .enabled(ignoreUnknownEnabled && !extensionsEnabled)
                .supportedOperations(List.of("*"))
                .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                .build());
  }

  @Bean
  @ConditionalOnProperty(
      name = "metadataChangeProposal.sideEffects.schemaField.enabled",
      havingValue = "true")
  public MCPSideEffect schemaFieldSideEffect() {
    AspectPluginConfig config =
        AspectPluginConfig.builder()
            .enabled(true)
            .className(SchemaFieldSideEffect.class.getName())
            .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE", "DELETE"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(Constants.DATASET_ENTITY_NAME)
                        .aspectName(Constants.STATUS_ASPECT_NAME)
                        .build(),
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(Constants.DATASET_ENTITY_NAME)
                        .aspectName(Constants.SCHEMA_METADATA_ASPECT_NAME)
                        .build()))
            .build();

    // prevent recursive dependency from using primary bean
    final EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry =
        new EntityChangeEventGeneratorRegistry();
    entityChangeEventGeneratorRegistry.register(
        SCHEMA_METADATA_ASPECT_NAME, new SchemaMetadataChangeEventGenerator());

    log.info("Initialized {}", SchemaFieldSideEffect.class.getName());
    return new SchemaFieldSideEffect()
        .setConfig(config)
        .setEntityChangeEventGeneratorRegistry(entityChangeEventGeneratorRegistry);
  }

  @Bean
  @ConditionalOnProperty(
      name = "metadataChangeProposal.sideEffects.dataProductUnset.enabled",
      havingValue = "true")
  public MCPSideEffect dataProductUnsetSideEffect() {
    AspectPluginConfig config =
        AspectPluginConfig.builder()
            .enabled(true)
            .className(DataProductUnsetSideEffect.class.getName())
            .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName(Constants.DATA_PRODUCT_ENTITY_NAME)
                        .aspectName(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                        .build()))
            .build();

    log.info("Initialized {}", SchemaFieldSideEffect.class.getName());
    return new DataProductUnsetSideEffect().setConfig(config);
  }

  @Bean
  public AspectPayloadValidator fieldPathValidator() {
    return new FieldPathValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(FieldPathValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE", "RESTATE"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator dataHubExecutionRequestResultValidator() {
    return new ExecutionRequestResultValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(ExecutionRequestResultValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of("UPSERT", "UPDATE"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                            .aspectName(EXECUTION_REQUEST_RESULT_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator hidePropertyValidator() {
    return new HidePropertyValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(HidePropertyValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator showPropertyAsAssetBadgeValidator() {
    return new ShowPropertyAsBadgeValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(ShowPropertyAsBadgeValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_SETTINGS_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator formPromptValidator() {
    return new FormPromptValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(FormPromptValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(FORM_ENTITY_NAME)
                            .aspectName(FORM_INFO_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator versionPropertiesValidator() {
    return new VersionPropertiesValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(VersionPropertiesValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(UPSERT, UPDATE, PATCH, CREATE, CREATE_ENTITY))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator versionSetPropertiesValidator() {
    return new VersionSetPropertiesValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(VersionSetPropertiesValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(UPSERT, UPDATE, PATCH, CREATE, CREATE_ENTITY))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(VERSION_SET_ENTITY_NAME)
                            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public MCPSideEffect versionPropertiesSideEffect() {
    return new VersionPropertiesSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .className(VersionPropertiesSideEffect.class.getName())
                .enabled(true)
                .supportedOperations(List.of(UPSERT, UPDATE, PATCH, CREATE, CREATE_ENTITY))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public MCPSideEffect versionSetSideEffect() {
    return new VersionSetSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .className(VersionSetSideEffect.class.getName())
                .enabled(true)
                .supportedOperations(List.of(UPSERT, UPDATE, PATCH, CREATE, CREATE_ENTITY))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(VERSION_SET_ENTITY_NAME)
                            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator urnAnnotationValidator() {
    return new UrnAnnotationValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(UrnAnnotationValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    // Special note: RESTATE is not included to allow out of order restoration of
                    // aspects.
                    List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY"))
                .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                .build());
  }

  @Bean
  public AspectPayloadValidator UserDeleteValidator() {
    return new UserDeleteValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(UserDeleteValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    // Special note: RESTATE is not included to allow out of order restoration of
                    // aspects.
                    List.of(DELETE))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(CORP_USER_ENTITY_NAME)
                            .aspectName(ALL)
                            .build()))
                .build());
  }

  @Bean
  @ConditionalOnProperty(
      name = "metadataChangeProposal.validation.privilegeConstraints.enabled",
      havingValue = "true")
  public AspectPayloadValidator privilegeConstraintsValidator() {
    // Supports tag constraints only for now
    return new PrivilegeConstraintsValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(PrivilegeConstraintsValidator.class.getName())
                .enabled(true)
                .supportedOperations(
                    List.of("UPSERT", "UPDATE", "CREATE", "CREATE_ENTITY", "RESTATE", "PATCH"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator ModifyIngestionSourceAuthValidator() {
    return new ModifyIngestionSourceAuthValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(ModifyIngestionSourceAuthValidator.class.getName())
                .enabled(true)
                .supportedOperations(AUTH_CHANGE_TYPE_OPERATIONS)
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(INGESTION_SOURCE_ENTITY_NAME)
                            .aspectName(INGESTION_INFO_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator ExecuteIngestionAuthValidator() {
    return new ExecuteIngestionAuthValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(ExecuteIngestionAuthValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, UPSERT, PATCH, UPDATE, CREATE_ENTITY))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(EXECUTION_REQUEST_ENTITY_NAME)
                            .aspectName(EXECUTION_REQUEST_INPUT_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator createIfNotExistsValidator() {
    return new CreateIfNotExistsValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(CreateIfNotExistsValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, CREATE_ENTITY))
                .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                .build());
  }

  @Bean
  public AspectPayloadValidator conditionalWriteValidator() {
    return new ConditionalWriteValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(ConditionalWriteValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, CREATE_ENTITY, DELETE, UPSERT, UPDATE, PATCH))
                .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
                .build());
  }

  @Bean
  public MutationHook fieldPathMutator() {
    return new FieldPathMutator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(FieldPathMutator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, UPSERT, UPDATE, RESTATE, PATCH))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(SCHEMA_METADATA_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public MutationHook ownershipOwnerTypes() {
    return new OwnershipOwnerTypes()
        .setConfig(
            AspectPluginConfig.builder()
                .className(OwnershipOwnerTypes.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, UPSERT, UPDATE, RESTATE, PATCH))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(OWNERSHIP_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public MutationHook structuredPropertiesSoftDelete() {
    return new StructuredPropertiesSoftDelete()
        .setConfig(
            AspectPluginConfig.builder()
                .className(StructuredPropertiesSoftDelete.class.getName())
                .enabled(true)
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator propertyDefinitionValidator() {
    return new PropertyDefinitionValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(PropertyDefinitionValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, CREATE_ENTITY, UPSERT))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_KEY_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator structuredPropertiesValidator() {
    return new StructuredPropertiesValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(StructuredPropertiesValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, UPSERT, DELETE))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(ALL)
                            .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public MCPSideEffect propertyDefinitionDeleteSideEffect() {
    return new PropertyDefinitionDeleteSideEffect()
        .setConfig(
            AspectPluginConfig.builder()
                .className(PropertyDefinitionDeleteSideEffect.class.getName())
                .enabled(true)
                .supportedOperations(List.of(DELETE))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME)
                            .build(),
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(STRUCTURED_PROPERTY_ENTITY_NAME)
                            .aspectName(STRUCTURED_PROPERTY_KEY_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator systemPolicyValidator(ConfigurationProvider configProvider) {
    PoliciesConfiguration policiesConfiguration = configProvider.getDatahub().getPolicies();
    Set<Urn> policyUrns = null;
    if (StringUtils.isNotBlank(policiesConfiguration.getSystemPolicyUrnList())) {
      List<String> urnStrings = List.of(policiesConfiguration.getSystemPolicyUrnList().split(","));
      policyUrns = urnStrings.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
    }
    return new SystemPolicyValidator()
        .setSystemPolicyUrns(policyUrns)
        .setConfig(
            AspectPluginConfig.builder()
                .className(SystemPolicyValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(DELETE, UPDATE, UPSERT, PATCH))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(POLICY_ENTITY_NAME)
                            .aspectName(ALL)
                            .build()))
                .build());
  }

  @Bean
  public AspectPayloadValidator policyFieldTypeValidator() {
    return new PolicyFieldTypeValidator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(PolicyFieldTypeValidator.class.getName())
                .enabled(true)
                .supportedOperations(List.of(CREATE, CREATE_ENTITY, UPSERT, UPDATE))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName(POLICY_ENTITY_NAME)
                            .aspectName(DATAHUB_POLICY_INFO_ASPECT_NAME)
                            .build()))
                .build());
  }

  @Bean
  public AspectSerializationHook aspectSizeValidationHook(
      ConfigurationProvider configProvider, AspectDao aspectDao) {
    AspectSizeValidationConfig config = configProvider.getDatahub().getValidation().getAspectSize();
    AspectSizeValidationHook hook = new AspectSizeValidationHook(aspectDao, config);
    log.info("Initialized AspectSizeValidationHook");
    return hook;
  }
}
