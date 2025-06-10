package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.Form;
import com.linkedin.datahub.graphql.generated.UpdateFormInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.datahub.graphql.types.form.FormMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormSettings;
import com.linkedin.form.FormState;
import com.linkedin.form.FormStatus;
import com.linkedin.form.FormType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.FormInfoPatchBuilder;
import com.linkedin.metadata.service.FormService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateFormResolver implements DataFetcher<CompletableFuture<Form>> {

  private final EntityClient _entityClient;
  private final FormService _formService;

  public UpdateFormResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final FormService formService) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Form> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final UpdateFormInput input =
        bindArgument(environment.getArgument("input"), UpdateFormInput.class);
    final Urn formUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.canManageForms(context)) {
              throw new AuthorizationException("Unable to update form. Please contact your admin.");
            }
            if (!_entityClient.exists(context.getOperationContext(), formUrn)) {
              throw new IllegalArgumentException(
                  String.format("Form with urn %s does not exist", formUrn));
            }

            FormInfo existingForm = getExistingForm(context, formUrn);
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
            if (input.getState() != null
                && !input
                    .getState()
                    .toString()
                    .equals(existingForm.getStatus().getState().toString())) {
              patchBuilder.setStatus(
                  createFormStatus(context.getOperationContext(), input.getState()));
            }
            if (input.getPromptsToAdd() != null) {
              patchBuilder.addPrompts(FormUtils.mapPromptsToAdd(input.getPromptsToAdd()));
            }
            if (input.getPromptsToRemove() != null) {
              patchBuilder.removePrompts(input.getPromptsToRemove());
            }
            if (input.getPrompts() != null) {
              patchBuilder.setPrompts(FormUtils.mapPromptsToAdd(input.getPrompts()));
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
              if (input.getActors().getUsers() != null) {
                patchBuilder.setAssignedUsers(input.getActors().getUsers());
              }
              if (input.getActors().getGroups() != null) {
                patchBuilder.setAssignedGroups(input.getActors().getGroups());
              }
            }
            patchBuilder.setLastModified(context.getOperationContext().getAuditStamp());
            try {
              _entityClient.ingestProposal(
                  context.getOperationContext(), patchBuilder.build(), false);
            } catch (IllegalArgumentException e) {
              log.info("Empty patch detected in updateFormResolver", e);
            }

            EntityResponse response =
                _entityClient.getV2(
                    context.getOperationContext(), Constants.FORM_ENTITY_NAME, formUrn, null);

            if (input.getFormAssetAssignment() != null) {
              DynamicFormAssignment dynamicFormAssignment =
                  FormUtils.updateDynamicFormAssignment(
                      context.getOperationContext(), response, input.getFormAssetAssignment());
              _formService.createDynamicFormAssignment(
                  context.getOperationContext(), dynamicFormAssignment, formUrn);
            }

            if (input.getFormSettings() != null) {
              FormSettings formSettings =
                  FormUtils.updateFormSettings(
                      context.getOperationContext(), response, input.getFormSettings());
              _formService.createFormSettings(context.getOperationContext(), formSettings, formUrn);
            }

            return FormMapper.map(context, response);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private FormStatus createFormStatus(
      OperationContext context, com.linkedin.datahub.graphql.generated.FormState state) {
    FormStatus formStatus = new FormStatus();
    formStatus.setState(FormState.valueOf(state.toString()));
    formStatus.setLastModified(context.getAuditStamp());
    return formStatus;
  }

  private FormInfo getExistingForm(@Nonnull final QueryContext context, @Nonnull final Urn formUrn)
      throws Exception {
    EntityResponse response =
        _entityClient.getV2(context.getOperationContext(), FORM_ENTITY_NAME, formUrn, null);

    if (response != null && response.getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
      return new FormInfo(response.getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
    }
    return null;
  }
}
