package com.datahub.graphql;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final Authentication authentication;
  private final Authorizer authorizer;
  @Getter private final String queryName;
  @Nonnull private final OperationContext operationContext;

  public SpringQueryContext(
      final boolean isAuthenticated,
      final Authentication authentication,
      final Authorizer authorizer,
      @Nonnull final OperationContext systemOperationContext,
      String jsonQuery,
      Map<String, Object> variables) {
    this.isAuthenticated = isAuthenticated;
    this.authentication = authentication;
    this.authorizer = authorizer;

    this.queryName =
        new Parser()
            .parseDocument(jsonQuery).getDefinitions().stream()
                .filter(def -> def instanceof OperationDefinition)
                .map(def -> (OperationDefinition) def)
                .filter(opDef -> opDef.getOperation().equals(OperationDefinition.Operation.QUERY))
                .findFirst()
                .map(OperationDefinition::getName)
                .orElse("graphql");

    this.operationContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder().buildGraphql(queryName, variables),
            authorizer,
            authentication,
            true);
  }
}
