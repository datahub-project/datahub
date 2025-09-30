package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.lineage.BackfillDatasetLineageIndexFields;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCondition.NonBlockingSystemUpdateCondition.class)
public class BackfillDatasetLineageIndexFieldsConfig {

  @Bean
  public NonBlockingSystemUpgrade backfillDatasetLineageIndexFields(
      final OperationContext opContext,
      final EntityService<?> entityService,
      final SearchService searchService,
      @Value("${systemUpdate.lineageIndexFields.enabled:true}") final boolean enabled,
      @Value("${systemUpdate.lineageIndexFields.reprocess.enabled:false}")
          final boolean reprocessEnabled,
      @Value("${systemUpdate.lineageIndexFields.batchSize:100}") final Integer batchSize) {
    return new BackfillDatasetLineageIndexFields(
        opContext, entityService, searchService, enabled, reprocessEnabled, batchSize);
  }
}
