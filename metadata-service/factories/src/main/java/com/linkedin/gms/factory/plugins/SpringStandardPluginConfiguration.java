package com.linkedin.gms.factory.plugins;

import com.linkedin.metadata.aspect.hooks.IgnoreUnknownMutator;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringStandardPluginConfiguration {

  @Value("${metadataChangeProposal.validation.ignoreUnknown}")
  private boolean ignoreUnknownEnabled;

  @Bean
  public MutationHook ignoreUnknownMutator() {
    return new IgnoreUnknownMutator()
        .setConfig(
            AspectPluginConfig.builder()
                .className(IgnoreUnknownMutator.class.getName())
                .enabled(ignoreUnknownEnabled)
                .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT"))
                .supportedEntityAspectNames(
                    List.of(
                        AspectPluginConfig.EntityAspectName.builder()
                            .entityName("*")
                            .aspectName("*")
                            .build()))
                .build());
  }
}
