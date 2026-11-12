package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.conditions.SystemUpdateCondition;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.datahub.upgrade.system.semanticsearch.CopyDocumentsToSemanticIndices;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.SearchV3ReadHold;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.ConfigurableEnvironment;

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
      final ConfigurableEnvironment environment) {
    // The configured property, not the live flag. The startup read hold may already have published
    // v2.enabled into the Environment when this bean is created. configuredBoolean ignores that
    // overlay, so a held V3 install does not start this copy. V3 document embeddings come from the
    // V3 indexer.
    boolean v2Enabled =
        SearchV3ReadHold.configuredBoolean(environment, SearchV3ReadHold.V2_ENABLED, false);

    return new CopyDocumentsToSemanticIndices(
        opContext,
        entityService,
        configurationProvider.getElasticSearch().getEntityIndex().getSemanticSearch(),
        indexConvention,
        enabled && v2Enabled);
  }
}
