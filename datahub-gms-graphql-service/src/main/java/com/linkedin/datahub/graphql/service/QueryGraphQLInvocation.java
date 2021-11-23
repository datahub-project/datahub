package com.linkedin.datahub.graphql.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.context.SpringQueryContext;

import graphql.ExecutionResult;
import graphql.spring.web.servlet.GraphQLInvocation;
import graphql.spring.web.servlet.GraphQLInvocationData;

@Deprecated
@Component
@Primary
public class QueryGraphQLInvocation implements GraphQLInvocation {

    @Autowired
    GraphQLEngine graphQLEngine;

    @Override
    public CompletableFuture<ExecutionResult> invoke(GraphQLInvocationData invocationData, WebRequest webRequest) {
        QueryContext queryContext = new SpringQueryContext(true, null, new AllowAllAuthorizer());

        return CompletableFuture.supplyAsync(() -> graphQLEngine.execute(invocationData.getQuery(),
            invocationData.getVariables(),
            queryContext));
    }
}
