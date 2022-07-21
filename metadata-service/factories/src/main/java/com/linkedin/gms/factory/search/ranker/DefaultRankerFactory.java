package com.linkedin.gms.factory.search.ranker;

import com.linkedin.metadata.search.ranker.DefaultRanker;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DefaultRankerFactory {
  @Bean(name = "defaultRanker")
  @Nonnull
  protected DefaultRanker getInstance() {
    return new DefaultRanker();
  }
}
