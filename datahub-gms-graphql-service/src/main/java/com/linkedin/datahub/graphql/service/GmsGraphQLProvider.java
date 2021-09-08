package com.linkedin.datahub.graphql.service;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;

import graphql.GraphQL;

@Component
public class GmsGraphQLProvider {

    private GraphQL graphQL;
    private GraphQLEngine graphQLEngine;

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

    @Bean
    public GraphQLEngine graphQLEngine() {
        return graphQLEngine;
    }

    @PostConstruct
    public void init() {
        this.graphQLEngine = new GmsGraphQLEngine().builder().build();
        this.graphQL = graphQLEngine.getGraphQL();
    }
}
