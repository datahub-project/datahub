package com.linkedin.gms.factory.search.ranker;

import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.search.ranker.DefaultRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class SearchRankerFactory {
  @Autowired
  private DefaultRanker _defaultRanker;

  @Value("${searchService.ranker.type}")
  private String rankerType;

  @Bean(name = "searchRanker")
  @Primary
  @Nonnull
  protected SearchRanker getInstance() {
    if (rankerType != null && rankerType.equals("SimpleRanker")) {
      return new SimpleRanker();
    }
    return _defaultRanker;
  }
}
