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
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AsyncBatchVerifyFormInput;
import com.linkedin.datahub.graphql.generated.AsyncBatchVerifyFormResponse;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AsyncBatchVerifyFormResolver
    implements DataFetcher<CompletableFuture<AsyncBatchVerifyFormResponse>> {

  private final FormService _formService;
  private final EntityClient _entityClient;
  private final MetadataTestClient _metadataTestClient;

  public AsyncBatchVerifyFormResolver(
      @Nonnull final FormService formService,
      @Nonnull final EntityClient entityClient,
      @Nonnull final MetadataTestClient metadataTestClient) {
    _formService = Objects.requireNonNull(formService, "formService must not be null");
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _metadataTestClient =
        Objects.requireNonNull(metadataTestClient, "metadataTestClient must not be null");
  }

  @Override
  public CompletableFuture<AsyncBatchVerifyFormResponse> get(
      final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final AsyncBatchVerifyFormInput input =
        bindArgument(environment.getArgument("input"), AsyncBatchVerifyFormInput.class);
    final @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs =
        EntitySpecUtils.getSearchableFieldsToPathSpecs(
            context.getOperationContext().getEntityRegistry(), getEntityNames(input.getTypes()));
    final TaskInput taskInput = input.getTaskInput();
    final String taskName = taskInput != null ? taskInput.getTaskName() : "Form verification";

    // create the new TestInfo aspect
    TestInfo testInfo = new TestInfo();
    testInfo.setName(taskName);
    testInfo.setCategory("Form Submission");
    TestSource testSource = new TestSource();
    testSource.setType(TestSourceType.BULK_FORM_SUBMISSION);
    Urn formUrn = UrnUtils.getUrn(input.getFormUrn());
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

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key, TEST_ENTITY_NAME, TEST_INFO_ASPECT_NAME, testInfo);
            String taskUrn =
                _entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            // evaluate the test right away
            FormUtils.runTest(context, _metadataTestClient, taskUrn);

            AsyncBatchVerifyFormResponse response = new AsyncBatchVerifyFormResponse();
            response.setTaskUrn(taskUrn);
            return response;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to async batch verify form with input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /*
   * Creates the JSON string for the test definition for the mega-bulk submit action
   */
  private String buildJsonTestDefinition(
      QueryContext context,
      AsyncBatchVerifyFormInput input,
      @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs) {
    Filter filter =
        FormUtils.combineFormFilterAndOrFilters(
            context, _formService, input.getOrFilters(), input.getFormFilter());
    ObjectNode definitionNode =
        FormUtils.buildBulkFormDefinitionOnNode(
            context, input.getTypes(), filter, searchableFieldsToPathSpecs);

    // add the action to take when something passes (everything should pass with no rules)
    ObjectMapper objectMapper = context.getOperationContext().getObjectMapper();
    ObjectNode actionsNode = definitionNode.putObject("actions");
    actionsNode.putArray("failing");
    ArrayNode passingArray = actionsNode.putArray("passing");
    ObjectNode submitPromptNode = passingArray.addObject();
    submitPromptNode.put("type", "VERIFY_FORM");
    ObjectNode verifyFormParamsNode = submitPromptNode.putObject("params");

    // add the params for the action
    verifyFormParamsNode.put("actorUrn", context.getActorUrn());
    verifyFormParamsNode.put("formUrn", input.getFormUrn());

    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(definitionNode);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to convert the input into a json definition %s", definitionNode),
          e);
    }
  }
}
