package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.semanticsearch.CopyDocumentsToSemanticIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
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
      final SearchClientShim<?> searchClient,
      final EntityService<?> entityService,
      final ConfigurationProvider configurationProvider,
      final IndexConvention indexConvention,
      @Value("${elasticsearch.entityIndex.semanticSearch.enabled:false}") boolean enabled) {

    return new CopyDocumentsToSemanticIndices(
        opContext,
        searchClient,
        entityService,
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch(),
        indexConvention,
        enabled);
  }
}
