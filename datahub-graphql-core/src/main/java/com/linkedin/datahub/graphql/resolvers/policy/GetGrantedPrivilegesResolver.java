package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.DataHubAuthorizer;
import com.datahub.authorization.ResourceSpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.GetGrantedPrivilegesInput;
import com.linkedin.datahub.graphql.generated.Privileges;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;


/**
 * Resolver to support the getGrantedPrivileges end point
 * Fetches all privileges that are granted for the given actor for the given resource (optional)
 */
public class GetGrantedPrivilegesResolver implements DataFetcher<CompletableFuture<Privileges>> {

  @Override
  public CompletableFuture<Privileges> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final GetGrantedPrivilegesInput input =
        bindArgument(environment.getArgument("input"), GetGrantedPrivilegesInput.class);
    final String actor = input.getActorUrn();
    if (!isAuthorized(context, actor)) {
      throw new AuthorizationException("Unauthorized to get privileges for the given author.");
    }
    final Optional<ResourceSpec> resourceSpec = Optional.ofNullable(input.getResourceSpec())
        .map(spec -> new ResourceSpec(EntityTypeMapper.getName(spec.getResourceType()), spec.getResourceUrn()));

    if (context.getAuthorizer() instanceof AuthorizerChain) {
      DataHubAuthorizer dataHubAuthorizer = ((AuthorizerChain) context.getAuthorizer()).getDefaultAuthorizer();
      List<String> privileges = dataHubAuthorizer.getGrantedPrivileges(actor, resourceSpec);
      return CompletableFuture.supplyAsync(() -> Privileges.builder()
          .setPrivileges(privileges)
          .build());
    }
    throw new UnsupportedOperationException(
        String.format("GetGrantedPrivileges function is not supported on authorizer of type %s",
            context.getAuthorizer().getClass().getSimpleName()));
  }

  private boolean isAuthorized(final QueryContext context, final String actor) {
    return actor.equals(context.getActorUrn());
  }
}