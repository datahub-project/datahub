package com.datahub.graphql;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final Authentication authentication;
  private final Authorizer authorizer;
  @Nonnull private final OperationContext operationContext;

  public SpringQueryContext(
      final boolean isAuthenticated,
      final Authentication authentication,
      final Authorizer authorizer,
      @Nonnull final OperationContext systemOperationContext) {
    this.isAuthenticated = isAuthenticated;
    this.authentication = authentication;
    this.authorizer = authorizer;
    this.operationContext =
        OperationContext.asSession(systemOperationContext, authorizer, authentication, true);
  }
}
