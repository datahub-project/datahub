package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchSubmitFormPromptInput;
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

public class BatchSubmitFormPromptResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final FormService _formService;

  public BatchSubmitFormPromptResolver(@Nonnull final FormService formService) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final BatchSubmitFormPromptInput input =
        bindArgument(environment.getArgument("input"), BatchSubmitFormPromptInput.class);
    final List<String> entityUrns = input.getAssetUrns();
    final SubmitFormPromptInput promptInput = input.getInput();
    final String promptId = promptInput.getPromptId();
    final Urn formUrn = UrnUtils.getUrn(promptInput.getFormUrn());
    final String fieldPath = promptInput.getFieldPath();
    final List<String> fieldPaths =
        promptInput.getFieldPaths() != null ? promptInput.getFieldPaths() : new ArrayList<>();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (promptInput.getType().equals(FormPromptType.STRUCTURED_PROPERTY)) {
              if (promptInput.getStructuredPropertyParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide structured property params for prompt type STRUCTURED_PROPERTY");
              }
              final Urn structuredPropertyUrn =
                  UrnUtils.getUrn(
                      promptInput.getStructuredPropertyParams().getStructuredPropertyUrn());
              final PrimitivePropertyValueArray values =
                  FormUtils.getStructuredPropertyValuesFromInput(promptInput);

              return _formService.batchSubmitStructuredPropertyPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()),
                  true);
            } else if (promptInput.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)) {
              if (promptInput.getStructuredPropertyParams() == null) {
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
                  UrnUtils.getUrn(
                      promptInput.getStructuredPropertyParams().getStructuredPropertyUrn());
              final PrimitivePropertyValueArray values =
                  FormUtils.getStructuredPropertyValuesFromInput(promptInput);

              return _formService.batchSubmitFieldStructuredPropertyPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId,
                  uniqueFieldPaths,
                  UrnUtils.getUrn(context.getActorUrn()));
            } else if (promptInput.getType().equals(FormPromptType.OWNERSHIP)) {
              if (promptInput.getOwnershipParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide ownership params for prompt type OWNERSHIP");
              }
              final List<Urn> owners =
                  promptInput.getOwnershipParams().getOwners().stream()
                      .map(UrnUtils::getUrn)
                      .collect(Collectors.toList());
              final Urn ownershipTypeUrn =
                  UrnUtils.getUrn(promptInput.getOwnershipParams().getOwnershipTypeUrn());
              return _formService.batchSubmitOwnershipPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  owners,
                  ownershipTypeUrn,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()),
                  true);
            } else if (promptInput.getType().equals(FormPromptType.DOCUMENTATION)) {
              if (promptInput.getDocumentationParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide documentation params for prompt type DOCUMENTATION");
              }
              final String documentation = promptInput.getDocumentationParams().getDocumentation();
              return _formService.batchSubmitDocumentationPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  documentation,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()),
                  true);
            } else if (promptInput.getType().equals(FormPromptType.GLOSSARY_TERMS)) {
              if (promptInput.getGlossaryTermsParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide glossary terms params for prompt type GLOSSARY_TERMS");
              }
              final List<Urn> termUrns =
                  promptInput.getGlossaryTermsParams().getGlossaryTermUrns().stream()
                      .map(UrnUtils::getUrn)
                      .collect(Collectors.toList());
              return _formService.batchSubmitGlossaryTermsPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  termUrns,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()),
                  true);
            } else if (promptInput.getType().equals(FormPromptType.DOMAIN)) {
              if (promptInput.getDomainParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide domain params for prompt type DOMAIN");
              }
              final Urn domainUrn = UrnUtils.getUrn(promptInput.getDomainParams().getDomainUrn());
              return _formService.batchSubmitDomainPromptResponse(
                  context.getOperationContext(),
                  entityUrns,
                  domainUrn,
                  formUrn,
                  promptId,
                  UrnUtils.getUrn(context.getActorUrn()),
                  true);
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
}
