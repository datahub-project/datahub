package com.linkedin.datahub.graphql.context;

import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.QueryContext;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@Component
public class SpringQueryContext implements QueryContext {

    boolean isAuthenticated;

    String actor;
}
