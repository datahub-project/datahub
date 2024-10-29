package com.linkedin.datahub.graphql.resolvers.ingest.source;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceConfigInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceInput;
import com.linkedin.datahub.graphql.generated.UpdateIngestionSourceScheduleInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.key.DataHubIngestionSourceKey;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;

/** Creates or updates an ingestion source. Requires the MANAGE_INGESTION privilege. */
@Slf4j
public class UpsertIngestionSourceResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;

  public UpsertIngestionSourceResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    if (!IngestionAuthUtils.canManageIngestion(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    final Optional<String> ingestionSourceUrn = Optional.ofNullable(environment.getArgument("urn"));
    final UpdateIngestionSourceInput input =
        bindArgument(environment.getArgument("input"), UpdateIngestionSourceInput.class);

    // Create the policy info.
    final DataHubIngestionSourceInfo info = mapIngestionSourceInfo(input);
    final MetadataChangeProposal proposal;
    if (ingestionSourceUrn.isPresent()) {
      // Update existing ingestion source
      try {
        proposal =
            buildMetadataChangeProposalWithUrn(
                Urn.createFromString(ingestionSourceUrn.get()), INGESTION_INFO_ASPECT_NAME, info);
      } catch (URISyntaxException e) {
        throw new DataHubGraphQLException(
            String.format("Malformed urn %s provided.", ingestionSourceUrn.get()),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    } else {
      // Create new ingestion source
      // Since we are creating a new Ingestion Source, we need to generate a unique UUID.
      final UUID uuid = UUID.randomUUID();
      final String uuidStr = uuid.toString();
      final DataHubIngestionSourceKey key = new DataHubIngestionSourceKey();
      key.setId(uuidStr);
      proposal =
          buildMetadataChangeProposalWithKey(
              key, INGESTION_SOURCE_ENTITY_NAME, INGESTION_INFO_ASPECT_NAME, info);
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to perform update against ingestion source with urn %s",
                    input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private DataHubIngestionSourceInfo mapIngestionSourceInfo(
      final UpdateIngestionSourceInput input) {
    final DataHubIngestionSourceInfo result = new DataHubIngestionSourceInfo();
    result.setType(input.getType());
    result.setName(input.getName());
    result.setConfig(mapConfig(input.getConfig()));
    if (input.getSchedule() != null) {
      result.setSchedule(mapSchedule(input.getSchedule()));
    }
    return result;
  }

  private DataHubIngestionSourceConfig mapConfig(final UpdateIngestionSourceConfigInput input) {
    final DataHubIngestionSourceConfig result = new DataHubIngestionSourceConfig();
    String recipe = input.getRecipe();
    result.setRecipe(recipe);
    if (input.getVersion() != null) {
      result.setVersion(input.getVersion());
    }
    if (input.getExecutorId() != null) {
      result.setExecutorId(input.getExecutorId());
    }
    if (input.getDebugMode() != null) {
      result.setDebugMode(input.getDebugMode());
    }
    if (input.getExtraArgs() != null) {
      Map<String, String> extraArgs =
          input.getExtraArgs().stream()
              .collect(
                  Collectors.toMap(StringMapEntryInput::getKey, StringMapEntryInput::getValue));
      result.setExtraArgs(new StringMap(extraArgs));
    }
    return result;
  }

  private DataHubIngestionSourceSchedule mapSchedule(
      final UpdateIngestionSourceScheduleInput input) {

    final String modifiedCronInterval = adjustCronInterval(input.getInterval());
    try {
      CronExpression.parse(modifiedCronInterval);
    } catch (IllegalArgumentException e) {
      throw new DataHubGraphQLException(
          String.format("Invalid cron schedule `%s`: %s", input.getInterval(), e.getMessage()),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    try {
      ZoneId.of(input.getTimezone());
    } catch (DateTimeException e) {
      throw new DataHubGraphQLException(
          String.format("Invalid timezone `%s`: %s", input.getTimezone(), e.getMessage()),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }

    final DataHubIngestionSourceSchedule result = new DataHubIngestionSourceSchedule();
    result.setInterval(input.getInterval());
    result.setTimezone(input.getTimezone());
    return result;
  }

  // Copied from IngestionScheduler.java
  private String adjustCronInterval(final String origCronInterval) {
    Objects.requireNonNull(origCronInterval, "origCronInterval must not be null");
    // Typically we support 5-character cron. Spring's lib only supports 6 character cron so we make
    // an adjustment here.
    final String[] originalCronParts = origCronInterval.split(" ");
    if (originalCronParts.length == 5) {
      return String.format("0 %s", origCronInterval);
    }
    return origCronInterval;
  }
}
