package com.linkedin.gms.factory.search.ranker;

import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.UsageBasedRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;


@Configuration
public class SearchRankerFactory {
  @Autowired
  private UsageBasedRanker usageBasedRanker;

  @Bean(name = "searchRanker")
  @Primary
  @Nonnull
  protected SearchRanker getInstance() {
    return usageBasedRanker;
  }
}
