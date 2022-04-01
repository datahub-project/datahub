package com.example.graphql.client.JavaGraphqlClient;


import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.api.Input;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;
import com.example.graphql.client.JavaGraphqlClient.type.EntityType;
import com.example.graphql.client.JavaGraphqlClient.type.SearchAcrossEntitiesInput;
import org.apache.commons.logging.Log;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

@SpringBootApplication
public class JavaGraphqlClientApplication {
    private static Logger logger = LoggerFactory.getLogger(JavaGraphqlClientApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(JavaGraphqlClientApplication.class, args);
        ApolloClient appolloClient = ApolloClient.builder().serverUrl("http://localhost:8080/api/graphql").build();
        appolloClient.query(new GetSearchResultsForMultipleQuery(SearchAcrossEntitiesInput.builder()
                .types(Arrays.asList(EntityType.DATASET))
                .query("myTag")
                .count(10)
                .build())).enqueue(
                new ApolloCall.Callback<GetSearchResultsForMultipleQuery.Data>() {

                    @Override
                    public void onResponse(@NotNull Response<GetSearchResultsForMultipleQuery.Data> response) {
                        logger.info(response.toString());
                    }

                    @Override
                    public void onFailure(@NotNull ApolloException e) {
                        logger.error(e.getMessage());
                    }

                });
    }

}
