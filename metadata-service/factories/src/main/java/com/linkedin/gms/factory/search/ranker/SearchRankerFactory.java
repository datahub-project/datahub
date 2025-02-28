package com.linkedin.gms.factory.search.ranker;

import com.linkedin.metadata.search.ranker.DefaultRanker;
import com.linkedin.metadata.search.ranker.OnlineUsageBasedRanker;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class SearchRankerFactory {
  public static final String SIMPLE_RANKER_TYPE = "SimpleRanker";

  public static final String ONLINE_USAGE_RANKER_TYPE = "OnlineUsageRanker";
  @Autowired private DefaultRanker _defaultRanker;

  @Autowired private OnlineUsageBasedRanker _usageBasedOnlineRanker;

  @Value("${searchService.ranker.type}")
  private String rankerType;

  @Bean(name = "searchRanker")
  @Primary
  @Nonnull
  protected SearchRanker getInstance() {
    if (rankerType != null && rankerType.equals(SIMPLE_RANKER_TYPE)) {
      return new SimpleRanker();
    }
    if (rankerType != null && rankerType.equals(ONLINE_USAGE_RANKER_TYPE)) {
      return _usageBasedOnlineRanker;
    }
    return _defaultRanker;
  }
}
