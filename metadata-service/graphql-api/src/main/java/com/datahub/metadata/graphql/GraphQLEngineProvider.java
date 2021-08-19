package com.datahub.metadata.graphql;

// import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;
import javax.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Bean;

@Component
public class GraphQLEngineProvider {

  private GraphQLEngine graphQLEngine;

  @Bean
  public GraphQLEngine graphQLEngine() {
    return graphQLEngine;
  }

  @PostConstruct
  public void init() {
    // this.graphQLEngine = new GmsGraphQLEngine().builder().build();
    this.graphQLEngine = null;
  }
}
