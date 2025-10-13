package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.dataprocessinstances.MigrateDataProcessInstanceEdges;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class MigrateDataProcessInstanceEdgesConfig {

  @Bean
  public NonBlockingSystemUpgrade migrateDataProcessInstanceEdges(
      final OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      @Value("${systemUpdate.migrateProcessInstanceEdges.enabled}") final boolean enabled,
      @Value("${systemUpdate.migrateProcessInstanceEdges.reprocess.enabled}")
          boolean reprocessEnabled,
      @Value("${systemUpdate.migrateProcessInstanceEdges.inputPlatforms}")
          final String inputPlatforms,
      @Value("${systemUpdate.migrateProcessInstanceEdges.outputPlatforms}")
          final String outputPlatforms,
      @Value("${systemUpdate.migrateProcessInstanceEdges.parentPlatforms}")
          final String parentPlatforms,
      @Value("${systemUpdate.migrateProcessInstanceEdges.batchSize}") final Integer batchSize) {
    return new MigrateDataProcessInstanceEdges(
        opContext,
        entityService,
        searchService,
        enabled,
        reprocessEnabled,
        List.of(inputPlatforms.split(",")),
        List.of(outputPlatforms.split(",")),
        List.of(parentPlatforms.split(",")),
        batchSize);
  }
}
