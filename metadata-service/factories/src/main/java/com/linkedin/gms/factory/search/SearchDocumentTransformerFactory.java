package com.linkedin.gms.factory.search;

import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SearchDocumentTransformerFactory {
  @Value("${elasticsearch.index.maxArrayLength}")
  private int maxArrayLength;

  @Value("${elasticsearch.index.maxObjectKeys}")
  private int maxObjectKeys;

  @Value("${elasticsearch.index.maxValueLength}")
  private int maxValueLength;

  @Bean("searchDocumentTransformer")
  protected SearchDocumentTransformer getInstance() {
    return new SearchDocumentTransformer(maxArrayLength, maxObjectKeys, maxValueLength);
  }
}
