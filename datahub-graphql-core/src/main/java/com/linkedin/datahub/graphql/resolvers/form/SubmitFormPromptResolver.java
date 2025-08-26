package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
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
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Null;

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

    return GraphQLConcurrencyUtils.supplyAsync(
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
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)) {
              if (input.getStructuredPropertyParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide structured property params for prompt type FIELDS_STRUCTURED_PROPERTY");
              }
              List<String> uniqueFieldPaths = getAndVerifyFieldPaths(fieldPath, fieldPaths);
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
                  uniqueFieldPaths,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.OWNERSHIP)) {
              if (input.getOwnershipParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide ownership params for prompt type OWNERSHIP");
              }
              final List<Urn> owners =
                  input.getOwnershipParams().getOwners().stream()
                      .map(UrnUtils::getUrn)
                      .collect(Collectors.toList());
              final Urn ownershipTypeUrn =
                  UrnUtils.getUrn(input.getOwnershipParams().getOwnershipTypeUrn());
              return _formService.submitOwnershipPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  owners,
                  ownershipTypeUrn,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.DOCUMENTATION)) {
              if (input.getDocumentationParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide documentation params for prompt type DOCUMENTATION");
              }
              final String documentation = input.getDocumentationParams().getDocumentation();
              return _formService.submitDocumentationPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  documentation,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.FIELDS_DOCUMENTATION)) {
              if (input.getDocumentationParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide documentation params for prompt type FIELDS_DOCUMENTATION");
              }
              List<String> uniqueFieldPaths = getAndVerifyFieldPaths(fieldPath, fieldPaths);

              final String documentation = input.getDocumentationParams().getDocumentation();
              return _formService.submitFieldDocumentationPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  documentation,
                  formUrn,
                  promptId,
                  uniqueFieldPaths,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.GLOSSARY_TERMS)) {
              if (input.getGlossaryTermsParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide glossary terms params for prompt type GLOSSARY_TERMS");
              }
              final List<Urn> termUrns =
                  input.getGlossaryTermsParams().getGlossaryTermUrns().stream()
                      .map(UrnUtils::getUrn)
                      .collect(Collectors.toList());
              return _formService.submitGlossaryTermsPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  termUrns,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.FIELDS_GLOSSARY_TERMS)) {
              if (input.getGlossaryTermsParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide glossary terms params for prompt type FIELDS_GLOSSARY_TERMS");
              }
              List<String> uniqueFieldPaths = getAndVerifyFieldPaths(fieldPath, fieldPaths);
              final List<Urn> termUrns =
                  input.getGlossaryTermsParams().getGlossaryTermUrns().stream()
                      .map(UrnUtils::getUrn)
                      .collect(Collectors.toList());
              return _formService.submitFieldGlossaryTermsPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  termUrns,
                  formUrn,
                  promptId,
                  uniqueFieldPaths,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (input.getType().equals(FormPromptType.DOMAIN)) {
              if (input.getDomainParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide domain params for prompt type DOMAIN");
              }
              final Urn domainUrn = UrnUtils.getUrn(input.getDomainParams().getDomainUrn());
              return _formService.submitDomainPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  domainUrn,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()));
            }
            return false;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
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

  private List<String> getAndVerifyFieldPaths(
      @Nullable final String fieldPath, @Null final List<String> fieldPaths) {
    if (fieldPath == null && fieldPaths.size() == 0) {
      throw new IllegalArgumentException("Failed to provide fieldPaths for FIELDS prompt type");
    }
    if (fieldPath != null) {
      fieldPaths.add(fieldPath);
    }
    return new ArrayList<>(new HashSet<>(fieldPaths));
  }
}
