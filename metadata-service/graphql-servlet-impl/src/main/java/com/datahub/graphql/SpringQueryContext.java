package com.datahub.graphql;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.context.RelationshipTraversalContext;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.config.GraphQLConfiguration;
import com.linkedin.metadata.config.graphql.GraphQLQueryConfiguration;
import com.linkedin.metadata.ratelimit.GraphqlDocumentMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.springframework.util.StringUtils;

@Getter
public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final Authentication authentication;
  private final Authorizer authorizer;
  @Getter private final String queryName;
  @Nonnull private final OperationContext operationContext;
  @Nonnull private final DataHubAppConfiguration dataHubAppConfig;
  @Nonnull private final RelationshipTraversalContext relationshipTraversalContext;
  private final int maxParentDepth;

  public SpringQueryContext(
      final boolean isAuthenticated,
      final Authentication authentication,
      final Authorizer authorizer,
      @Nonnull final OperationContext systemOperationContext,
      @Nonnull final DataHubAppConfiguration dataHubAppConfig,
      @Nonnull final HttpServletRequest request,
      @Nonnull final GraphqlDocumentMetadata documentMetadata,
      Map<String, Object> variables,
      @Nonnull GraphqlUsageClassificationRegistry graphqlUsageClassificationRegistry) {
    this.isAuthenticated = isAuthenticated;
    this.authentication = authentication;
    this.authorizer = authorizer;
    this.queryName = documentMetadata.resolvedOperationName();

    GraphQLConfiguration graphQL =
        Objects.requireNonNull(
            dataHubAppConfig.getGraphQL(),
            "graphQL configuration is required; define graphQL in application.yaml");
    GraphQLQueryConfiguration queryConfig =
        Objects.requireNonNull(
            graphQL.getQuery(),
            "graphQL.query configuration is required; define graphQL.query in application.yaml");
    this.relationshipTraversalContext =
        new RelationshipTraversalContext(queryConfig.getMaxVisitedUrns());
    this.maxParentDepth = queryConfig.getMaxParentDepth();

    GraphqlRequestUsageClassifier.Result classification =
        GraphqlRequestUsageClassifier.classify(
            documentMetadata, graphqlUsageClassificationRegistry);

    this.operationContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildGraphql(authentication.getActor().toUrnStr(), request, queryName, variables)
                .withUsageOperation(classification.usageOperation())
                .graphqlOperationKind(classification.kind()),
            authorizer,
            authentication,
            true);

    this.dataHubAppConfig = dataHubAppConfig;
  }

  @Override
  public int getMaxParentDepth() {
    return maxParentDepth;
  }

  @Override
  public Optional<RelationshipTraversalContext> getRelationshipTraversalContext() {
    return Optional.of(relationshipTraversalContext);
  }
}
