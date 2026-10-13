package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.Objects;
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
  protected SearchDocumentTransformer getInstance(ConfigurationProvider configurationProvider) {
    StructuredPropertiesConfiguration structuredProperties =
        Objects.requireNonNull(
            configurationProvider.getStructuredProperties(),
            "structuredProperties configuration is required");
    int keywordMaxLength = structuredProperties.getKeywordMaxLength();
    if (keywordMaxLength <= 0) {
      keywordMaxLength = ESUtils.KEYWORD_MAXLENGTH;
    }
    return new SearchDocumentTransformer(
        maxArrayLength,
        maxObjectKeys,
        maxValueLength,
        structuredProperties.isDropOversizedKeywordValuesFromIndex(),
        keywordMaxLength);
  }
}
