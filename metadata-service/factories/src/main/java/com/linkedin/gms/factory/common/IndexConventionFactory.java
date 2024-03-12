package com.linkedin.gms.factory.common;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Creates a {@link IndexConvention} to generate search index names.
 *
 * <p>This allows you to easily add prefixes to the index names.
 */
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class IndexConventionFactory {
  public static final String INDEX_CONVENTION_BEAN = "searchIndexConvention";

  @Value("${elasticsearch.index.prefix:}")
  private String indexPrefix;

  @Bean(name = INDEX_CONVENTION_BEAN)
  protected IndexConvention createInstance() {
    return new IndexConventionImpl(indexPrefix);
  }
}
