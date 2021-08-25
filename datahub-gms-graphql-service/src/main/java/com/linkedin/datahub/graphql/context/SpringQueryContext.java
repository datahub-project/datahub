package com.linkedin.datahub.graphql.context;

import com.datahub.metadata.authorization.Authorizer;
import com.linkedin.datahub.graphql.service.MockAuthorizationManager;
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

    Authorizer authorizer;

    @Override
    public Authorizer getAuthorizer() {
        return this.authorizer;
    }
}
