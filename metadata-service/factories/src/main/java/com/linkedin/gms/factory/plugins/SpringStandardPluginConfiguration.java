package com.linkedin.gms.factory.plugins;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.hooks.ExtendedModelStructuredPropertyMutator;
import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.config.structuredProperties.extensions.ExtendedModelValidationConfiguration;
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
      name = "metadataChangeProposal.validation.extensions.enabled",
      havingValue = "true")
  public MutationHook extendedModelStructuredPropertyMutator(
      ConfigurationProvider configurationProvider) throws Exception {
    ExtendedModelValidationConfiguration config =
        configurationProvider
            .getMetadataChangeProposal()
            .getValidation()
            .getExtensions()
            .resolve(new YAMLMapper());
    return new ExtendedModelStructuredPropertyMutator(config, extensionsEnabled);
  }
}
