package com.linkedin.metadata.graphql.api;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;

import graphql.GraphQL;

@Component
public class GmsGraphQLController {

    private GraphQL graphQL;

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

    @PostConstruct
    public void init() {
        GraphQLEngine graphQLEngineBuilder = GmsGraphQLEngine.builder().build();
        this.graphQL = graphQLEngineBuilder.getGraphQL();
    }
}
