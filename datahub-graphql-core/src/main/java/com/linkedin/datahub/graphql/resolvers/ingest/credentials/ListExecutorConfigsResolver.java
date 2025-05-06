package com.linkedin.datahub.graphql.resolvers.ingest.credentials;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ExecutorConfigs;
import com.linkedin.datahub.graphql.generated.ListExecutorConfigsInput;
import com.linkedin.datahub.graphql.generated.ListExecutorConfigsResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.*;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.config.ExecutorConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@Slf4j
/** Lists all Signal Requests given a list of Execution Request urns. */
public class ListExecutorConfigsResolver
    implements DataFetcher<CompletableFuture<ListExecutorConfigsResult>> {
  private final EntityClient _entityClient;
  private final StsClient _stsClient;
  private final ExecutorConfiguration _executorConfiguration;

  /**
   * This API is now supposed to be used in such a way that it returns at most one item, that is,
   * the client is normally expected to request config of one specific Executor Pool, but as a
   * potential optimization, it's allowed to request up to MAX_ITEMS_PER_BATCH in one request
   */
  private final int MAX_ITEMS_PER_BATCH = 100;

  public ListExecutorConfigsResolver(
      @Nonnull final EntityClient entityClient,
      final StsClient stsClient,
      @Nonnull final ExecutorConfiguration executorConfiguration) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _stsClient = stsClient;
    _executorConfiguration =
        Objects.requireNonNull(executorConfiguration, "executorConfiguration must not be null");
  }

  private void listExecutorConfigs(
      QueryContext context,
      List<String> executorIds,
      Credentials myCreds,
      List<ExecutorConfigs> executorConfigList) {
    try {
      Filter filter = null;
      if (!executorIds.isEmpty()) {
        List<String> executorUrns =
            executorIds.stream()
                .map(
                    s ->
                        String.format(
                            "urn:li:%s:%s", AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, s))
                .collect(Collectors.toList());

        ConjunctiveCriterionArray conjunctiveCriteria = new ConjunctiveCriterionArray();
        final CriterionArray executorIdsAndArray = new CriterionArray();
        executorIdsAndArray.add(buildCriterion("urn", Condition.EQUAL, executorUrns));
        conjunctiveCriteria.add(new ConjunctiveCriterion().setAnd(executorIdsAndArray));
        filter = new Filter().setOr(conjunctiveCriteria);
      }

      final SearchResult gmsResult =
          _entityClient.search(
              context.getOperationContext(),
              AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME,
              "*",
              filter,
              null,
              0,
              MAX_ITEMS_PER_BATCH);

      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME,
              new HashSet<>(
                  gmsResult.getEntities().stream()
                      .map(SearchEntity::getEntity)
                      .collect(Collectors.toList())),
              ImmutableSet.of(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME));

      gmsResult.getEntities().stream()
          .forEach(
              entity -> {
                EntityResponse resp = entities.get(entity.getEntity());
                if (!resp.hasAspects()
                    || !resp.getAspects()
                        .containsKey(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME)) {
                  return;
                }
                RemoteExecutorPoolInfo poolInfo =
                    new RemoteExecutorPoolInfo(
                        resp.getAspects()
                            .get(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME)
                            .getValue()
                            .data());

                /** Do not return embedded or non-ready pools */
                if (poolInfo.isIsEmbedded()
                    || poolInfo.getState().getStatus() != RemoteExecutorPoolStatus.READY) {
                  log.warn(
                      String.format(
                          "Not returning executor pool entry %s: isEmbedded:%s; status:%s",
                          entity.getEntity().getId(),
                          poolInfo.isIsEmbedded(),
                          poolInfo.getState().getStatus()));
                  return;
                }

                final ExecutorConfigs executorConfig = new ExecutorConfigs();

                executorConfig.setExecutorId(entity.getEntity().getId());
                executorConfig.setQueueUrl(poolInfo.getQueueUrl());
                executorConfig.setRegion(poolInfo.getQueueRegion());

                executorConfig.setAccessKeyId(myCreds.accessKeyId());
                executorConfig.setSecretKeyId(myCreds.secretAccessKey());
                executorConfig.setSessionToken(myCreds.sessionToken());
                executorConfig.setExpiration(myCreds.expiration().toString());
                executorConfigList.add(executorConfig);
              });
    } catch (Exception e) {
      throw new RuntimeException("Failed to list executor pools", e);
    }
  }

  @Override
  public CompletableFuture<ListExecutorConfigsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final ListExecutorConfigsInput input =
        bindArgument(environment.getArgument("input"), ListExecutorConfigsInput.class);

    final List<String> executorIds =
        input != null && input.getExecutorIds() != null
            ? input.getExecutorIds()
            : Collections.<String>emptyList();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          List<ExecutorConfigs> executorConfigList = new ArrayList<>();
          try {
            // If AWS_* variables are not passed or incorrect, do not hard fail.
            // Instead, log a descriptive error and return empty config.
            if (_stsClient == null) {
              throw new IllegalArgumentException(
                  "STS client is null. Make sure AWS_REGION is configured correctly.");
            }
            AssumeRoleRequest roleRequest =
                AssumeRoleRequest.builder()
                    .roleArn(this._executorConfiguration.executorRoleArn)
                    .roleSessionName("remote-executor-session")
                    .build();
            AssumeRoleResponse roleResponse = _stsClient.assumeRole(roleRequest);
            Credentials myCreds = roleResponse.credentials();

            listExecutorConfigs(context, executorIds, myCreds, executorConfigList);
          } catch (Exception e) {
            log.error(String.format("Failed to list executor configs: %s", e.toString()));
            throw new RuntimeException("Failed to list executor configs!", e);
          } finally {
            final ListExecutorConfigsResult result = new ListExecutorConfigsResult();
            result.setTotal(executorConfigList.size());
            result.setExecutorConfigs(executorConfigList);
            return result;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
