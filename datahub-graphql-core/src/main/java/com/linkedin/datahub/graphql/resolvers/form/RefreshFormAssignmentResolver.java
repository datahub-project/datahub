package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RefreshFormAssignmentInput;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RefreshFormAssignmentResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final FormService _formService;

  public RefreshFormAssignmentResolver(@Nonnull final FormService formService) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final RefreshFormAssignmentInput input =
        bindArgument(environment.getArgument("input"), RefreshFormAssignmentInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getUrn());
    final Boolean reassignAllAssets =
        input.getReassignAllAssets() != null ? input.getReassignAllAssets() : false;
    final Boolean unassignForm = input.getUnassignForm() != null ? input.getUnassignForm() : true;

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageForms(context)) {
              throw new AuthorizationException(
                  "Unable to reassign form. Please contact your admin.");
            }
            _formService.refreshFormAssignment(
                context.getOperationContext(), formUrn, reassignAllAssets, unassignForm);
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to reassign form", e);
          }
        });
  }
}
