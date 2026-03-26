package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({BaseElasticSearchComponentsFactory.class})
public class ElasticSearchSystemMetadataServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Bean(name = "esSystemMetadataDAO")
  @Nonnull
  public ESSystemMetadataDAO esSystemMetadataDAO(
      final ConfigurationProvider configurationProvider) {
    return new ESSystemMetadataDAO(
        components.getSearchClient(),
        components.getIndexConvention(),
        components.getBulkProcessor(),
        components.getConfig().getBulkProcessor().getNumRetries(),
        configurationProvider.getSystemMetadataService());
  }

  @Bean(name = "elasticSearchSystemMetadataService")
  @Nonnull
  protected ElasticSearchSystemMetadataService getInstance(
      @Qualifier("esSystemMetadataDAO") final ESSystemMetadataDAO esSystemMetadataDAO,
      @Value("${elasticsearch.idHashAlgo}") final String elasticIdHashAlgo,
      final ConfigurationProvider configurationProvider) {
    return new ElasticSearchSystemMetadataService(
        components.getBulkProcessor(),
        components.getIndexConvention(),
        esSystemMetadataDAO,
        components.getIndexBuilder(),
        elasticIdHashAlgo,
        configurationProvider.getSystemMetadataService());
  }
}
