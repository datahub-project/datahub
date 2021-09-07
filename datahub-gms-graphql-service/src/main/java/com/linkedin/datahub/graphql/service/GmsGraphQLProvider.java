package com.linkedin.datahub.graphql.service;

import com.linkedin.metadata.entity.EntityService;
import javax.annotation.PostConstruct;

import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.GraphQLEngine;

import graphql.GraphQL;

@Component
public class GmsGraphQLProvider {

    private GraphQL graphQL;
    private GraphQLEngine graphQLEngine;

    @Inject
    @Named("entityService")
    private EntityService _entityService;

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
        this.graphQLEngine = new GmsGraphQLEngine().builder().configureEntityService(_entityService).build();
        this.graphQL = graphQLEngine.getGraphQL();
    }
}
