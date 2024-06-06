package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.structured.PrimitivePropertyValueArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class SubmitFormPromptResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final FormService _formService;

  public SubmitFormPromptResolver(@Nonnull final FormService formService) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final Urn entityUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final SubmitFormPromptInput input =
        bindArgument(environment.getArgument("input"), SubmitFormPromptInput.class);
    final String promptId = input.getPromptId();
    final Urn formUrn = UrnUtils.getUrn(input.getFormUrn());
    final String fieldPath = input.getFieldPath();
    final List<String> fieldPaths =
        input.getFieldPaths() != null ? input.getFieldPaths() : new ArrayList<>();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            // ensure the user is assigned to this form/entity before letting them submit a prompt
            checkUserIsAssigned(context, formUrn, entityUrn);

            if (input.getType().equals(FormPromptType.STRUCTURED_PROPERTY)) {
              if (input.getStructuredPropertyParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide structured property params for prompt type STRUCTURED_PROPERTY");
              }
              final Urn structuredPropertyUrn =
                  UrnUtils.getUrn(input.getStructuredPropertyParams().getStructuredPropertyUrn());
              final PrimitivePropertyValueArray values =
                  FormUtils.getStructuredPropertyValuesFromInput(input);

              return _formService.submitStructuredPropertyPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId);
            } else if (input.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)) {
              if (input.getStructuredPropertyParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide structured property params for prompt type FIELDS_STRUCTURED_PROPERTY");
              }
              if (fieldPath == null && fieldPaths.size() == 0) {
                throw new IllegalArgumentException(
                    "Failed to provide fieldPaths for prompt type FIELDS_STRUCTURED_PROPERTY");
              }
              if (fieldPath != null) {
                fieldPaths.add(fieldPath);
              }
              List<String> uniqueFieldPaths = new ArrayList<>(new HashSet<>(fieldPaths));
              final Urn structuredPropertyUrn =
                  UrnUtils.getUrn(input.getStructuredPropertyParams().getStructuredPropertyUrn());
              final PrimitivePropertyValueArray values =
                  FormUtils.getStructuredPropertyValuesFromInput(input);

              return _formService.submitFieldStructuredPropertyPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId,
                  uniqueFieldPaths);
            }
            return false;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }

  private void checkUserIsAssigned(
      @Nonnull final QueryContext context, @Nonnull final Urn formUrn, @Nonnull final Urn entityUrn)
      throws Exception {
    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

    final List<Urn> groupsForUser =
        _formService.getGroupsForUser(context.getOperationContext(), actorUrn);
    if (!_formService.isFormAssignedToUser(
        context.getOperationContext(), formUrn, entityUrn, actorUrn, groupsForUser)) {
      throw new AuthorizationException(
          String.format(
              "Failed to authorize form on entity as form with urn %s is not assigned to user",
              formUrn));
    }
  }
}
