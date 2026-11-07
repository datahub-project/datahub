package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.semanticsearch.CopyDocumentsToSemanticIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Conditional(SystemUpdateCondition.BlockingSystemUpdateCondition.class)
public class CopyDocumentsToSemanticIndicesConfig {

  @Order(3) // After BuildIndices (order 2)
  @Bean(name = "copyDocumentsToSemanticIndices")
  public BlockingSystemUpgrade copyDocumentsToSemanticIndices(
      @Qualifier("systemOperationContext") final OperationContext opContext,
      final EntityService<?> entityService,
      final ConfigurationProvider configurationProvider,
      final IndexConvention indexConvention,
      @Value("${elasticsearch.entityIndex.semanticSearch.enabled}") boolean enabled,
      // The configured property, not the live flag: a startup read hold turns V2 on in memory, and
      // that must not start this copy. V3 document embeddings come from the V3 indexer.
      @Value("${elasticsearch.entityIndex.v2.enabled}") boolean v2Enabled) {

    return new CopyDocumentsToSemanticIndices(
        opContext,
        entityService,
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch(),
        indexConvention,
        enabled && v2Enabled);
  }
}
