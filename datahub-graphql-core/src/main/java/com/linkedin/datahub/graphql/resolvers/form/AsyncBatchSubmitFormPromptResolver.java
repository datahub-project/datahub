package com.linkedin.datahub.graphql.resolvers.form;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithKey;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getEntityNames;
import static com.linkedin.metadata.Constants.TEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.TEST_INFO_ASPECT_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AsyncBatchSubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.AsyncBatchSubmitFormPromptResponse;
import com.linkedin.datahub.graphql.generated.FormPromptType;
import com.linkedin.datahub.graphql.generated.SubmitFormPromptInput;
import com.linkedin.datahub.graphql.generated.TaskInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.FormUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.service.FormService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.MetadataTestClient;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestInterval;
import com.linkedin.test.TestSchedule;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AsyncBatchSubmitFormPromptResolver
    implements DataFetcher<CompletableFuture<AsyncBatchSubmitFormPromptResponse>> {

  private final FormService _formService;
  private final EntityClient _entityClient;
  private final MetadataTestClient _metadataTestClient;

  public AsyncBatchSubmitFormPromptResolver(
      @Nonnull final FormService formService,
      @Nonnull final EntityClient entityClient,
      @Nonnull final MetadataTestClient metadataTestClient) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _metadataTestClient =
        Objects.requireNonNull(metadataTestClient, "metadataTestClient must not be null");
  }

  @Override
  public CompletableFuture<AsyncBatchSubmitFormPromptResponse> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final AsyncBatchSubmitFormPromptInput input =
        bindArgument(environment.getArgument("input"), AsyncBatchSubmitFormPromptInput.class);
    final @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs =
        EntitySpecUtils.getSearchableFieldsToPathSpecs(
            context.getOperationContext().getEntityRegistry(), getEntityNames(input.getTypes()));
    final TaskInput taskInput = input.getTaskInput();
    final String taskName =
        taskInput != null ? taskInput.getTaskName() : "Form question submission";

    // create the new TestInfo aspect
    TestInfo testInfo = new TestInfo();
    testInfo.setName(taskName);
    testInfo.setCategory("Form Submission");
    TestSource testSource = new TestSource();
    testSource.setType(TestSourceType.BULK_FORM_SUBMISSION);
    Urn formUrn = UrnUtils.getUrn(input.getInput().getFormUrn());
    testSource.setSourceEntity(formUrn);
    testInfo.setSource(testSource);
    TestSchedule testSchedule = new TestSchedule();
    testSchedule.setInterval(TestInterval.NONE);
    testInfo.setSchedule(testSchedule);
    com.linkedin.test.TestDefinition definition = new com.linkedin.test.TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    definition.setJson(buildJsonTestDefinition(context, input, searchableFieldsToPathSpecs));
    if (input.getQuery() != null) {
      definition.setOnQuery(input.getQuery());
    }
    testInfo.setDefinition(definition);

    final TestKey key = new TestKey();
    key.setId(UUID.randomUUID().toString());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key, TEST_ENTITY_NAME, TEST_INFO_ASPECT_NAME, testInfo);
            String taskUrn =
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            // evaluate the test right away
            FormUtils.runTest(context, _metadataTestClient, taskUrn);

            AsyncBatchSubmitFormPromptResponse response = new AsyncBatchSubmitFormPromptResponse();
            response.setTaskUrn(taskUrn);
            return response;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to async batch submit form prompt with input %s", input), e);
          }
        });
  }

  /*
   * Creates the JSON string for the test definition for the mega-bulk submit action
   */
  private String buildJsonTestDefinition(
      QueryContext context,
      AsyncBatchSubmitFormPromptInput input,
      @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs) {
    Filter filter =
        FormUtils.combineFormFilterAndOrFilters(
            context, _formService, input.getOrFilters(), input.getFormFilter());
    ObjectNode definitionNode =
        FormUtils.buildBulkFormDefinitionOnNode(
            context, input.getTypes(), filter, searchableFieldsToPathSpecs);

    // add the action to take when something passes (everything should pass with no rules)
    ObjectMapper objectMapper = context.getOperationContext().getObjectMapper();
    SubmitFormPromptInput promptInput = input.getInput();
    ObjectNode actionsNode = definitionNode.putObject("actions");
    actionsNode.putArray("failing");
    ArrayNode passingArray = actionsNode.putArray("passing");
    ObjectNode submitPromptNode = passingArray.addObject();
    submitPromptNode.put("type", "SUBMIT_FORM_PROMPT");
    ObjectNode submitPromptParamsNode = submitPromptNode.putObject("params");

    // add the params for the action
    submitPromptParamsNode.put("actorUrn", context.getActorUrn());
    submitPromptParamsNode.put("formUrn", promptInput.getFormUrn());
    submitPromptParamsNode.put("promptId", promptInput.getPromptId());
    submitPromptParamsNode.put("promptType", promptInput.getType().toString());
    if (promptInput.getType().equals(FormPromptType.STRUCTURED_PROPERTY)) {
      addStructuredPropertyPromptParams(submitPromptParamsNode, promptInput);
    } else if (promptInput.getType().equals(FormPromptType.OWNERSHIP)) {
      addOwnershipPromptParams(submitPromptParamsNode, promptInput);
    } else if (promptInput.getType().equals(FormPromptType.DOCUMENTATION)) {
      addDocumentationPromptParams(submitPromptParamsNode, promptInput);
    } else if (promptInput.getType().equals(FormPromptType.GLOSSARY_TERMS)) {
      addGlossaryTermsPromptParams(submitPromptParamsNode, promptInput);
    } else if (promptInput.getType().equals(FormPromptType.DOMAIN)) {
      addDomainPromptParams(submitPromptParamsNode, promptInput);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Failed to submit, unexpected prompt type submitted %s.", promptInput.getType()));
    }

    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(definitionNode);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to convert the input into a json definition %s", definitionNode),
          e);
    }
  }

  /*
   * Adds the params necessary to submit a structured prop response
   */
  private void addStructuredPropertyPromptParams(
      ObjectNode submitPromptParamsNode, SubmitFormPromptInput promptInput)
      throws IllegalArgumentException {
    if (promptInput.getStructuredPropertyParams() == null) {
      throw new IllegalArgumentException(
          "Failed to submit as no structured property params were provided for structured prop response");
    }
    submitPromptParamsNode.put(
        "structuredPropertyUrn",
        promptInput.getStructuredPropertyParams().getStructuredPropertyUrn());
    List<String> values = new ArrayList<>();
    if (promptInput.getStructuredPropertyParams().getValues().get(0).getNumberValue() != null) {
      promptInput
          .getStructuredPropertyParams()
          .getValues()
          .forEach(v -> values.add(v.getNumberValue().toString()));
      ArrayNode valuesArray = submitPromptParamsNode.putArray("numberValues");
      values.forEach(valuesArray::add);
    } else {
      promptInput
          .getStructuredPropertyParams()
          .getValues()
          .forEach(v -> values.add(v.getStringValue()));
      ArrayNode valuesArray = submitPromptParamsNode.putArray("stringValues");
      values.forEach(valuesArray::add);
    }
    if (values.size() == 0) {
      throw new IllegalArgumentException(
          "Failed to submit as no values for structured property response were provided.");
    }
  }

  /*
   * Adds the params necessary to submit an ownership response
   */
  private void addOwnershipPromptParams(
      ObjectNode submitPromptParamsNode, SubmitFormPromptInput promptInput)
      throws IllegalArgumentException {
    if (promptInput.getOwnershipParams() == null) {
      throw new IllegalArgumentException(
          "Failed to submit as no ownership params were provided for ownership response");
    }
    ArrayNode ownersArray = submitPromptParamsNode.putArray("owners");
    promptInput.getOwnershipParams().getOwners().forEach(ownersArray::add);
    submitPromptParamsNode.put(
        "ownershipTypeUrn", promptInput.getOwnershipParams().getOwnershipTypeUrn());
  }

  /*
   * Adds the params necessary to submit a documentation response
   */
  private void addDocumentationPromptParams(
      ObjectNode submitPromptParamsNode, SubmitFormPromptInput promptInput)
      throws IllegalArgumentException {
    if (promptInput.getDocumentationParams() == null) {
      throw new IllegalArgumentException(
          "Failed to submit as no documentation params were provided for documentation response");
    }
    submitPromptParamsNode.put(
        "documentation", promptInput.getDocumentationParams().getDocumentation());
  }

  /*
   * Adds the params necessary to submit a glossary terms response
   */
  private void addGlossaryTermsPromptParams(
      ObjectNode submitPromptParamsNode, SubmitFormPromptInput promptInput)
      throws IllegalArgumentException {
    if (promptInput.getGlossaryTermsParams() == null) {
      throw new IllegalArgumentException(
          "Failed to submit as no glossary terms params were provided for glossary terms response");
    }
    ArrayNode termsArray = submitPromptParamsNode.putArray("glossaryTerms");
    promptInput.getGlossaryTermsParams().getGlossaryTermUrns().forEach(termsArray::add);
  }

  /*
   * Adds the params necessary to submit a domain response
   */
  private void addDomainPromptParams(
      ObjectNode submitPromptParamsNode, SubmitFormPromptInput promptInput)
      throws IllegalArgumentException {
    if (promptInput.getDomainParams() == null) {
      throw new IllegalArgumentException(
          "Failed to submit as no domain params were provided for domain response");
    }
    submitPromptParamsNode.put("domain", promptInput.getDomainParams().getDomainUrn());
  }
}
