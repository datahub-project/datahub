package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.RoleUser;
import com.linkedin.datahub.graphql.generated.UserFilter;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ActorUsersResolver implements DataFetcher<CompletableFuture<ArrayList<RoleUser>>> {

  private final EntityClient _client;

  @Override
  public CompletableFuture<ArrayList<RoleUser>> get(final DataFetchingEnvironment environment)
      throws Exception {
    final UserFilter userFilter = bindArgument(environment.getArgument("input"), UserFilter.class);

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (userFilter != null) {
              Urn userUrn = UrnUtils.getUrn(userFilter.getUserUrn());
              EntityResponse userEntityResponse =
                  _client.getV2(CORP_USER_ENTITY_NAME, userUrn, null, context.getAuthentication());
              if (userEntityResponse == null) {
                return null;
              }
              CorpUser corpUser = CorpUserMapper.map(userEntityResponse);
              RoleUser roleUser = new RoleUser();
              roleUser.setUser(corpUser);
              ArrayList<RoleUser> roleUsers = new ArrayList<>();
              roleUsers.add(roleUser);
              return roleUsers;
            } else {
              ListResult userEntityResponse = _client.list(context.getOperationContext(), CORP_USER_ENTITY_NAME, null, 0, 10_000);
              ArrayList<RoleUser> roleUsers = new ArrayList<>();
              for (Urn urn: userEntityResponse.getEntities()) {
                CorpUser corpUser = new CorpUser();
                corpUser.setUrn(urn.toString());
                RoleUser roleUser = new RoleUser();
                roleUser.setUser(corpUser);
                roleUsers.add(roleUser);
              }
              return roleUsers;
            }
          } catch (RemoteInvocationException | URISyntaxException e) {
            throw new RuntimeException("Failed to retrieve aspects from GMS", e);
          }
        });
  }
}
