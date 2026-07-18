package com.linkedin.gms.factory.search.filter;

import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriter;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueryFilterRewriterChainFactory {

  /**
   * All {@link QueryFilterRewriter} beans (for example expansion rewriters contributed only when ES
   * is enabled).
   */
  @Bean
  public QueryFilterRewriteChain queryFilterRewriteChain(List<QueryFilterRewriter> rewriters) {
    return new QueryFilterRewriteChain(rewriters);
  }
}
