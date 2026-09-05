package com.linkedin.gms.factory.systemmetadata;

import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.systemmetadata.scroll.ESSystemMetadataScrollClient;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ESSystemMetadataScrollClientFactory {

  @Bean(name = "esSystemMetadataScrollClient")
  @Nonnull
  @Conditional(SystemMetadataElasticsearchBackendCondition.class)
  public ESSystemMetadataScrollClient esSystemMetadataScrollClient(
      @Qualifier("esSystemMetadataDAO") ESSystemMetadataDAO esSystemMetadataDAO) {
    return new ESSystemMetadataScrollClient(esSystemMetadataDAO);
  }
}
