package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchSubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.structured.PrimitivePropertyValueArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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

    return CompletableFuture.supplyAsync(
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
                  entityUrns,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId,
                  context.getAuthentication());
            } else if (promptInput.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)) {
              if (promptInput.getStructuredPropertyParams() == null) {
                throw new IllegalArgumentException(
                    "Failed to provide structured property params for prompt type FIELDS_STRUCTURED_PROPERTY");
              }
              if (fieldPath == null) {
                throw new IllegalArgumentException(
                    "Failed to provide fieldPath for prompt type FIELDS_STRUCTURED_PROPERTY");
              }
              final Urn structuredPropertyUrn =
                  UrnUtils.getUrn(
                      promptInput.getStructuredPropertyParams().getStructuredPropertyUrn());
              final PrimitivePropertyValueArray values =
                  FormUtils.getStructuredPropertyValuesFromInput(promptInput);

              return _formService.batchSubmitFieldStructuredPropertyPromptResponse(
                  entityUrns,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId,
                  fieldPath,
                  context.getAuthentication());
            }
            return false;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }
}
