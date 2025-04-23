package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithKey;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.NotificationConnectionTestRequestInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.integrations.invoker.JSON;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateNotificationConnectionTestResolver
    implements DataFetcher<CompletableFuture<String>> {

  private final String TEST_EVENT_NOTIFICATION_TITLE = "Hello from DataHub!";
  private final String TEST_EVENT_NOTIFICATION_BODY = "This is a test notification requested by %s";
  private static final String TEST_CONNECTION_TASK_NAME =
      Constants.NOTIFICATION_CONNECTION_TEST_EXECUTION_REQUEST_TASK_NAME;
  private static final String TEST_CONNECTION_SOURCE_NAME = "MANUAL_TEST_CONNECTION";

  private final EntityClient entityClient;

  public CreateNotificationConnectionTestResolver(@Nonnull final EntityClient entityClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient cannot be null");
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {

    // 1. extract details from query context
    final QueryContext context = environment.getContext();
    final NotificationConnectionTestRequestInput input =
        bindArgument(
            environment.getArgument("input"), NotificationConnectionTestRequestInput.class);

    // 2. extract query input
    final Urn connectionUrn = UrnUtils.getUrn(input.getUrn());
    final SlackNotificationSettingsInput maybeSlackSettingsInput = input.getSlack();

    // 3. generate connection-specific args
    final StringMap args = new StringMap();
    if (maybeSlackSettingsInput != null) {
      if (maybeSlackSettingsInput.getChannels() != null) {
        args.put(
            "channelsJSON", JSON.serialize(new StringArray(maybeSlackSettingsInput.getChannels())));
      }
      if (maybeSlackSettingsInput.getUserHandle() != null) {
        args.put("userHandle", maybeSlackSettingsInput.getUserHandle());
      }
      args.put("title", TEST_EVENT_NOTIFICATION_TITLE);
      args.put("body", String.format(TEST_EVENT_NOTIFICATION_BODY, context.getActor().getId()));
    }

    // 4. Generate the execution request
    final ExecutionRequestKey key = new ExecutionRequestKey();
    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    key.setId(uuidStr);

    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    execInput.setTask(TEST_CONNECTION_TASK_NAME);
    execInput.setSource(
        new ExecutionRequestSource()
            .setType(TEST_CONNECTION_SOURCE_NAME)
            .setConnection(connectionUrn));

    // We use an executorId that maps to no executor since some requests are to be handled by the
    // mae-consumer
    execInput.setExecutorId(Constants.NONE_EXECUTOR_ID);
    execInput.setRequestedAt(System.currentTimeMillis());
    execInput.setActorUrn(UrnUtils.getUrn(context.getActorUrn()));
    execInput.setArgs(args);

    // 4. Generate the request and return the run
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithKey(
                    key,
                    EXECUTION_REQUEST_ENTITY_NAME,
                    EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                    execInput);
            return entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to generate a execution request for Connection with urn %s.",
                    connectionUrn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
