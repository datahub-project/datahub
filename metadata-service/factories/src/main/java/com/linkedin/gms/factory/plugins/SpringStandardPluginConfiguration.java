package com.linkedin.gms.factory.plugins;

import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.schemafields.sideeffects.SchemaFieldSideEffect;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.timeline.eventgenerator.SchemaMetadataChangeEventGenerator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SpringStandardPluginConfiguration {

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
                .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName("*")
                            .aspectName("*")
                            .build()))
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
}
