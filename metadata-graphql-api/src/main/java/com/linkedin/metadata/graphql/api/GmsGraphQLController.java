package com.linkedin.metadata.graphql.api;

import javax.annotation.PostConstruct;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.GmsGraphQLEngine;
import com.linkedin.datahub.graphql.resolvers.AuthenticatedResolver;
import com.linkedin.datahub.graphql.resolvers.corpuser.ManagerResolver;
import com.linkedin.datahub.graphql.resolvers.ownership.OwnerResolver;
import com.linkedin.datahub.graphql.resolvers.query.DatasetResolver;

import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import static com.linkedin.datahub.graphql.Constants.CORP_USER_TYPE_NAME;
import static com.linkedin.datahub.graphql.Constants.DATASETS_FIELD_NAME;
import static com.linkedin.datahub.graphql.Constants.MANAGER_FIELD_NAME;
import static com.linkedin.datahub.graphql.Constants.OWNER_FIELD_NAME;
import static com.linkedin.datahub.graphql.Constants.OWNER_TYPE_NAME;
import static com.linkedin.datahub.graphql.Constants.QUERY_TYPE_NAME;

import static graphql.Scalars.GraphQLLong;

@Component
public class GmsGraphQLController {

    private GraphQL graphQL;

    @Bean
    public GraphQL graphQL() {
        return graphQL;
    }

    @PostConstruct
    public void init() {
        String schema = GmsGraphQLEngine.schema();
        GraphQLSchema graphQLSchema = buildSchema(schema);
        this.graphQL = GraphQL.newGraphQL(graphQLSchema).build();
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        RuntimeWiring runtimeWiring = buildWiring();
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring buildWiring() {
        return RuntimeWiring.newRuntimeWiring()
            .type(QUERY_TYPE_NAME, typeWiring -> typeWiring
                .dataFetcher(DATASETS_FIELD_NAME, new AuthenticatedResolver<>(new DatasetResolver()))
            )
            .type(OWNER_TYPE_NAME, typeWiring -> typeWiring
                .dataFetcher(OWNER_FIELD_NAME, new AuthenticatedResolver<>(new OwnerResolver()))
            )
            .type(CORP_USER_TYPE_NAME, typeWiring -> typeWiring
                .dataFetcher(MANAGER_FIELD_NAME, new AuthenticatedResolver<>(new ManagerResolver()))
            )
            .scalar(GraphQLLong)
            .build();
    }
}
