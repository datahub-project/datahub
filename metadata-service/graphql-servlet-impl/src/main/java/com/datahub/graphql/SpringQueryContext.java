package com.datahub.graphql;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.language.OperationDefinition;
import graphql.parser.Parser;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

@Getter
public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final Authentication authentication;
  private final Authorizer authorizer;
  @Getter private final String queryName;
  @Nonnull private final OperationContext operationContext;
  @Nonnull private final DataHubAppConfiguration dataHubAppConfig;

  public SpringQueryContext(
      final boolean isAuthenticated,
      final Authentication authentication,
      final Authorizer authorizer,
      @Nonnull final OperationContext systemOperationContext,
      @Nonnull final DataHubAppConfiguration dataHubAppConfig,
      @Nonnull final HttpServletRequest request,
      @Nullable final String operationName,
      String jsonQuery,
      Map<String, Object> variables) {
    this.isAuthenticated = isAuthenticated;
    this.authentication = authentication;
    this.authorizer = authorizer;

    // operationName is an optional field only required if multiple operations are present
    this.queryName =
        operationName != null
            ? operationName
            : new Parser()
                .parseDocument(jsonQuery).getDefinitions().stream()
                    .filter(def -> def instanceof OperationDefinition)
                    .map(def -> (OperationDefinition) def)
                    .filter(
                        opDef -> opDef.getOperation().equals(OperationDefinition.Operation.QUERY))
                    .findFirst()
                    .map(OperationDefinition::getName)
                    .orElse("graphql");

    this.operationContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildGraphql(authentication.getActor().toUrnStr(), request, queryName, variables),
            authorizer,
            authentication,
            true);

    this.dataHubAppConfig = dataHubAppConfig;
  }
}
