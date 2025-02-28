package com.linkedin.datahub.graphql.resolvers.ingest.credentials;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ExecutorConfigs;
import com.linkedin.datahub.graphql.generated.ListExecutorConfigsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.ExecutorConfiguration;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.paginators.ListQueuesIterable;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Lists all Signal Requests given a list of Execution Request urns. */
public class ListExecutorConfigsResolver
    implements DataFetcher<CompletableFuture<ListExecutorConfigsResult>> {
  private final EntityClient _entityClient;
  private final ExecutorConfiguration _executorConfiguration;

  public ListExecutorConfigsResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final ExecutorConfiguration executorConfiguration) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _executorConfiguration =
        Objects.requireNonNull(executorConfiguration, "executorConfiguration must not be null");
  }

  private void processQueuesByPrefix(
      SqsClient sqsClient,
      String queueNamePrefix,
      Region region,
      Credentials myCreds,
      List<ExecutorConfigs> executorConfigList) {
    ListQueuesRequest listQueuesRequest =
        ListQueuesRequest.builder().queueNamePrefix(queueNamePrefix).build();
    ListQueuesIterable listQueues = sqsClient.listQueuesPaginator(listQueuesRequest);
    listQueues.stream()
        .flatMap(r -> r.queueUrls().stream())
        .forEach(
            queueUrl -> {
              String[] urlParts = queueUrl.split("/");
              String urlName = urlParts[urlParts.length - 1];
              String executorId = urlName.substring(queueNamePrefix.length() + 1);

              final ExecutorConfigs executorConfig = new ExecutorConfigs();
              executorConfig.setExecutorId(executorId);
              executorConfig.setQueueUrl(queueUrl);
              executorConfig.setRegion(region.toString());
              executorConfig.setAccessKeyId(myCreds.accessKeyId());
              executorConfig.setSecretKeyId(myCreds.secretAccessKey());
              executorConfig.setSessionToken(myCreds.sessionToken());
              executorConfig.setExpiration(myCreds.expiration().toString());
              executorConfigList.add(executorConfig);
            });
  }

  @Override
  public CompletableFuture<ListExecutorConfigsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          List<ExecutorConfigs> executorConfigList = new ArrayList<>();

          try {
            StsClient stsClient = StsClient.create();

            AssumeRoleRequest roleRequest =
                AssumeRoleRequest.builder()
                    .roleArn(this._executorConfiguration.executorRoleArn)
                    .roleSessionName("remote-executor-session")
                    .build();

            AssumeRoleResponse roleResponse = stsClient.assumeRole(roleRequest);
            Credentials myCreds = roleResponse.credentials();

            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            SqsClient sqsClient = SqsClient.create();

            String[] prefixes = {
              "le-" + this._executorConfiguration.executorCustomerId,
              "re-" + this._executorConfiguration.executorCustomerId
            };
            for (String prefix : prefixes) {
              processQueuesByPrefix(sqsClient, prefix, region, myCreds, executorConfigList);
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to list executor configs!", e);
          } finally {
            final ListExecutorConfigsResult result = new ListExecutorConfigsResult();
            result.setTotal(executorConfigList.size());
            result.setExecutorConfigs(executorConfigList);
            return result;
          }
        });
  }
}
