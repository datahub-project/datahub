package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SearchDocumentTransformerFactory {
  @Value("${elasticsearch.index.maxArrayLength}")
  private int maxArrayLength;

  @Value("${elasticsearch.index.maxObjectKeys}")
  private int maxObjectKeys;

  @Bean("searchDocumentTransformer")
  protected SearchDocumentTransformer getInstance() {
    return new SearchDocumentTransformer(maxArrayLength, maxObjectKeys);
  }
}
