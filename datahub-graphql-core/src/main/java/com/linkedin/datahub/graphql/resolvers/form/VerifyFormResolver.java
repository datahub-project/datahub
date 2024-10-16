package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.VerifyFormInput;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class VerifyFormResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final FormService _formService;
  private final GroupService _groupService;

  public VerifyFormResolver(
      @Nonnull final FormService formService, @Nonnull final GroupService groupService) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
    _groupService = Objects.requireNonNull(groupService, "groupService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final VerifyFormInput input =
        bindArgument(environment.getArgument("input"), VerifyFormInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getFormUrn());
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final Authentication authentication = context.getAuthentication();
    final Urn actorUrn = UrnUtils.getUrn(authentication.getActor().toUrnStr());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final List<Urn> groupsForUser =
                _groupService.getGroupsForUser(context.getOperationContext(), actorUrn);
            if (!_formService.isFormAssignedToUser(
                context.getOperationContext(), formUrn, entityUrn, actorUrn, groupsForUser)) {
              throw new AuthorizationException(
                  String.format(
                      "Failed to authorize form on entity as form with urn %s is not assigned to user",
                      formUrn));
            }
            _formService.verifyFormForEntity(context.getOperationContext(), formUrn, entityUrn);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
