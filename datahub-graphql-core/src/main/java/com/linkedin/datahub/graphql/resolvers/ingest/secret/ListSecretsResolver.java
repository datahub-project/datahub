package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ListSecretsInput;
import com.linkedin.datahub.graphql.generated.ListSecretsResult;
import com.linkedin.datahub.graphql.generated.Secret;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Lists all secrets present within DataHub. Requires the MANAGE_SECRETS privilege.
 */
@Slf4j
public class ListSecretsResolver implements DataFetcher<CompletableFuture<ListSecretsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListSecretsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListSecretsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageSecrets(context)) {
      final ListSecretsInput input = bindArgument(environment.getArgument("input"), ListSecretsInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all secrets
          final ListResult gmsResult = _entityClient.list(Constants.SECRETS_ENTITY_NAME, Collections.emptyMap(), start, count, context.getAuthentication());

          // Then, resolve all secrets
          final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
              Constants.SECRETS_ENTITY_NAME,
              new HashSet<>(gmsResult.getEntities()),
              ImmutableSet.of(Constants.SECRET_VALUE_ASPECT_NAME),
              context.getAuthentication());

          // Now that we have entities we can bind this to a result.
          final ListSecretsResult result = new ListSecretsResult();
          result.setStart(gmsResult.getStart());
          result.setCount(gmsResult.getCount());
          result.setTotal(gmsResult.getTotal());
          result.setSecrets(mapEntities(entities.values()));
          return result;

        } catch (Exception e) {
          throw new RuntimeException("Failed to list secrets", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<Secret> mapEntities(final Collection<EntityResponse> entities) {
    final List<Secret> results = new ArrayList<>();
    for (EntityResponse response : entities) {
      final Urn entityUrn = response.getUrn();
      final EnvelopedAspectMap aspects = response.getAspects();

      // There should ALWAYS be a value aspect.
      final EnvelopedAspect envelopedInfo = aspects.get(Constants.SECRET_VALUE_ASPECT_NAME);

      // Bind into a strongly typed object.
      final DataHubSecretValue secretValue = new DataHubSecretValue(envelopedInfo.getValue().data());

      // Map using the strongly typed object.
      results.add(mapSecretValue(entityUrn, secretValue));
    }
    return results;
  }

  private Secret mapSecretValue(final Urn urn, final DataHubSecretValue value) {
    final Secret result = new Secret();
    result.setUrn(urn.toString());
    result.setName(value.getName());
    result.setDescription(value.getDescription(GetMode.NULL));
    return result;
  }
}