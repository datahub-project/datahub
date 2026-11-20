package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.search.transformer.SearchDocumentEnricher;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SearchDocumentTransformerFactory {
  @Value("${elasticsearch.index.maxArrayLength}")
  private int maxArrayLength;

  @Value("${elasticsearch.index.maxObjectKeys}")
  private int maxObjectKeys;

  @Value("${elasticsearch.index.maxValueLength}")
  private int maxValueLength;

  @Bean("searchDocumentTransformer")
  protected SearchDocumentTransformer getInstance(
      ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Nullable List<SearchDocumentEnricher> enrichers) {
    StructuredPropertiesConfiguration structuredProperties =
        Objects.requireNonNull(
            configurationProvider.getStructuredProperties(),
            "structuredProperties configuration is required");
    int keywordMaxLength = structuredProperties.getKeywordMaxLength();
    if (keywordMaxLength <= 0) {
      keywordMaxLength = ESUtils.KEYWORD_MAXLENGTH;
    }
    SearchDocumentTransformer transformer =
        new SearchDocumentTransformer(
            maxArrayLength,
            maxObjectKeys,
            maxValueLength,
            structuredProperties.isDropOversizedKeywordValuesFromIndex(),
            keywordMaxLength);
    transformer.setEnrichers(enrichers);
    if (configurationProvider.getElasticSearch() != null
        && configurationProvider.getElasticSearch().getSearch() != null
        && configurationProvider.getElasticSearch().getSearch().getV3() != null
        && configurationProvider
            .getElasticSearch()
            .getSearch()
            .getV3()
            .isBusinessAttributeExpansionEnabled()) {
      transformer.setV3BusinessAttributeExpansionEnabled(true);
      log.info("Search V3 business attribute expansion is enabled");
    }
    return transformer;
  }
}
