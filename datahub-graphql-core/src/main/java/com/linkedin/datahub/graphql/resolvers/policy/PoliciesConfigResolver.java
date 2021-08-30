package com.linkedin.datahub.graphql.resolvers.policy;

import com.datahub.metadata.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.PoliciesConfig;
import com.linkedin.datahub.graphql.generated.Privilege;
import com.linkedin.datahub.graphql.generated.ResourcePrivileges;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class PoliciesConfigResolver implements DataFetcher<CompletableFuture<PoliciesConfig>> {

  @Override
  public CompletableFuture<PoliciesConfig> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final PoliciesConfig config = new PoliciesConfig();

    boolean policiesEnabled = Authorizer.AuthorizationMode.DEFAULT.equals(context.getAuthorizer().mode());
    config.setEnabled(policiesEnabled);

    config.setPlatformPrivileges(com.linkedin.datahub.graphql.authorization.PoliciesConfig.PLATFORM_PRIVILEGES
        .stream()
        .map(this::mapPrivilege)
        .collect(Collectors.toList()));

    config.setMetadataPrivileges(com.linkedin.datahub.graphql.authorization.PoliciesConfig.RESOURCE_PRIVILEGES
        .stream()
        .map(this::mapResourcePrivileges)
        .collect(Collectors.toList())
    );

    return CompletableFuture.completedFuture(config);
  }

  private ResourcePrivileges mapResourcePrivileges(
      com.linkedin.datahub.graphql.authorization.PoliciesConfig.ResourcePrivileges resourcePrivileges) {
    final ResourcePrivileges graphQLPrivileges = new ResourcePrivileges();
    graphQLPrivileges.setResourceType(resourcePrivileges.getResourceType());
    graphQLPrivileges.setResourceTypeDisplayName(resourcePrivileges.getResourceTypeDisplayName());
    graphQLPrivileges.setPrivileges(
        resourcePrivileges.getPrivileges().stream().map(this::mapPrivilege).collect(Collectors.toList())
    );
    return graphQLPrivileges;
  }

  private Privilege mapPrivilege(com.linkedin.datahub.graphql.authorization.PoliciesConfig.Privilege privilege) {
    final Privilege graphQLPrivilege = new Privilege();
    graphQLPrivilege.setType(privilege.getType());
    graphQLPrivilege.setDisplayName(privilege.getDisplayName());
    graphQLPrivilege.setDescription(privilege.getDescription());
    return graphQLPrivilege;
  }
}