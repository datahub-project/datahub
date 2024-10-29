package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.UpdateFormInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.datahub.graphql.types.form.FormMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.FormType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.FormInfoPatchBuilder;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class UpdateFormResolver implements DataFetcher<CompletableFuture<Form>> {

  private final EntityClient _entityClient;

  public UpdateFormResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<Form> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final UpdateFormInput input =
        bindArgument(environment.getArgument("input"), UpdateFormInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageForms(context)) {
              throw new AuthorizationException("Unable to update form. Please contact your admin.");
            }
            if (!_entityClient.exists(context.getOperationContext(), formUrn)) {
              throw new IllegalArgumentException(
                  String.format("Form with urn %s does not exist", formUrn));
            }

            FormInfoPatchBuilder patchBuilder = new FormInfoPatchBuilder().urn(formUrn);
            if (input.getName() != null) {
              patchBuilder.setName(input.getName());
            }
            if (input.getDescription() != null) {
              patchBuilder.setDescription(input.getDescription());
            }
            if (input.getType() != null) {
              patchBuilder.setType(FormType.valueOf(input.getType().toString()));
            }
            if (input.getPromptsToAdd() != null) {
              patchBuilder.addPrompts(FormUtils.mapPromptsToAdd(input.getPromptsToAdd()));
            }
            if (input.getPromptsToRemove() != null) {
              patchBuilder.removePrompts(input.getPromptsToRemove());
            }
            if (input.getActors() != null) {
              if (input.getActors().getOwners() != null) {
                patchBuilder.setOwnershipForm(input.getActors().getOwners());
              }
              if (input.getActors().getUsersToAdd() != null) {
                input.getActors().getUsersToAdd().forEach(patchBuilder::addAssignedUser);
              }
              if (input.getActors().getUsersToRemove() != null) {
                input.getActors().getUsersToRemove().forEach(patchBuilder::removeAssignedUser);
              }
              if (input.getActors().getGroupsToAdd() != null) {
                input.getActors().getGroupsToAdd().forEach(patchBuilder::addAssignedGroup);
              }
              if (input.getActors().getGroupsToRemove() != null) {
                input.getActors().getGroupsToRemove().forEach(patchBuilder::removeAssignedGroup);
              }
            }
            _entityClient.ingestProposal(
                context.getOperationContext(), patchBuilder.build(), false);

            EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(), Constants.FORM_ENTITY_NAME, formUrn, null);
            return FormMapper.map(context, response);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }
}
