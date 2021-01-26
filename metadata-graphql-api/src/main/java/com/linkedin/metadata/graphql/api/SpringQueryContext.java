package com.linkedin.metadata.graphql.api;

import org.springframework.stereotype.Component;

import com.linkedin.datahub.graphql.QueryContext;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Component
public class SpringQueryContext implements QueryContext {

    boolean isAuthenticated;

    String actor;
}
