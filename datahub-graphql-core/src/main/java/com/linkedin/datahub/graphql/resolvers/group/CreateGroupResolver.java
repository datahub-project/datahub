package com.linkedin.datahub.graphql.resolvers.group;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateGroupInput;
import com.linkedin.metadata.key.CorpGroupKey;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


// Currently, this resolver will override the group details, but not group membership, if a group with the same name already exists.
public class CreateGroupResolver implements DataFetcher<CompletableFuture<String>> {

  private final GroupService _groupService;

  public CreateGroupResolver(final GroupService groupService) {
    _groupService = groupService;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    Authentication authentication = context.getAuthentication();

    if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    final CreateGroupInput input = bindArgument(environment.getArgument("input"), CreateGroupInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {
        // First, check if the group already exists.
        // Create the Group key.
        final CorpGroupKey key = new CorpGroupKey();
        final String id = input.getId() != null ? input.getId() : UUID.randomUUID().toString();
        key.setName(id); // 'name' in the key really reflects nothing more than a stable "id".
        return _groupService.createNativeGroup(key, input.getName(), input.getDescription(), authentication);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create group", e);
      }
    });
  }
}