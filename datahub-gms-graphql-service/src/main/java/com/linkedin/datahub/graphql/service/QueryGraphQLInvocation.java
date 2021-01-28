package com.linkedin.datahub.graphql.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.WebRequest;

import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.context.SpringQueryContext;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.spring.web.servlet.GraphQLInvocation;
import graphql.spring.web.servlet.GraphQLInvocationData;

@Component
@Primary
public class QueryGraphQLInvocation implements GraphQLInvocation {

    @Autowired
    GraphQL graphQL;

    @Autowired
    GraphQLEngine graphQLEngine;

    public static final String APPNAME = "GmsGraphQLApp";

    @Override
    public CompletableFuture<ExecutionResult> invoke(GraphQLInvocationData invocationData, WebRequest webRequest) {
        QueryContext queryContext = new SpringQueryContext(true, APPNAME);

        ExecutionInput executionInput = ExecutionInput.newExecutionInput()
            .query(invocationData.getQuery())
            .operationName(invocationData.getOperationName())
            .variables(invocationData.getVariables())
            .dataLoaderRegistry(graphQLEngine.get_registry())
            .context(queryContext)
            .build();
        return graphQL.executeAsync(executionInput);
    }
}
