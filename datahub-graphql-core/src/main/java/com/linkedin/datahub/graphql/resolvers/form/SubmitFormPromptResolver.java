package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.structured.PrimitivePropertyValueArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collection;
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

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (input.getStructuredPropertyParams() == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "Failed to provide structured property params for prompt type %s",
                      input.getType()));
            }
            if (input.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)
                && fieldPath == null) {
              throw new IllegalArgumentException(
                  "Failed to provide fieldPath for prompt type FIELDS_STRUCTURED_PROPERTY");
            }
            final Urn structuredPropertyUrn =
                UrnUtils.getUrn(input.getStructuredPropertyParams().getStructuredPropertyUrn());
            final PrimitivePropertyValueArray values =
                FormUtils.getStructuredPropertyValuesFromInput(input);

            // A field prompt writes the property on the schema field, so that is the URN the
            // privilege check has to see. The dataset check accepts Edit Entity Properties, which
            // the schema-field write does not.
            final Urn propertyTarget =
                input.getType().equals(FormPromptType.FIELDS_STRUCTURED_PROPERTY)
                    ? SchemaFieldUtils.generateSchemaFieldUrn(
                        entityUrn, Objects.requireNonNull(fieldPath))
                    : entityUrn;
            authorize(
                context,
                entityUrn,
                propertyTarget,
                formUrn,
                promptId,
                input.getType(),
                structuredPropertyUrn);

            if (input.getType().equals(FormPromptType.STRUCTURED_PROPERTY)) {
              return _formService.submitStructuredPropertyPromptResponse(
                  context.getOperationContext(),
                  entityUrn,
                  structuredPropertyUrn,
                  values,
                  formUrn,
                  promptId);
            }
            return _formService.submitFieldStructuredPropertyPromptResponse(
                context.getOperationContext(),
                entityUrn,
                structuredPropertyUrn,
                values,
                formUrn,
                promptId,
                fieldPath);
          } catch (AuthorizationException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * The response must answer a prompt on a form assigned to the entity, and set the property that
   * prompt asks for. An assigned user or group may answer without a property privilege. A caller
   * who is not assigned may answer only when they may edit that property on {@code propertyTarget}:
   * the entity for an entity prompt, the schema field for a field prompt.
   */
  private void authorize(
      @Nonnull final QueryContext context,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn propertyTarget,
      @Nonnull final Urn formUrn,
      @Nonnull final String promptId,
      @Nonnull final FormPromptType promptType,
      @Nonnull final Urn structuredPropertyUrn)
      throws Exception {
    if (!_formService.isPromptForStructuredProperty(
        context.getOperationContext(),
        formUrn,
        entityUrn,
        promptId,
        com.linkedin.form.FormPromptType.valueOf(promptType.name()),
        structuredPropertyUrn)) {
      throw new AuthorizationException(
          String.format(
              "Prompt %s on form %s does not ask for %s on %s",
              promptId, formUrn, structuredPropertyUrn, entityUrn));
    }
    Collection<Urn> groups = context.getOperationContext().getActorGroups();
    if (_formService.isFormAssignedToUser(
        context.getOperationContext(),
        formUrn,
        entityUrn,
        UrnUtils.getUrn(context.getActorUrn()),
        groups == null ? List.of() : List.copyOf(groups))) {
      return;
    }
    if (!AuthorizationUtils.canEditStructuredProperties(
        propertyTarget, context, List.of(structuredPropertyUrn))) {
      throw new AuthorizationException(
          String.format("Unauthorized to edit structured properties on %s", propertyTarget));
    }
  }
}
