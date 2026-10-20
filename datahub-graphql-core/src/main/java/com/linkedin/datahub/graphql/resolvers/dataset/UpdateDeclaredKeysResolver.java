package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.DeclaredKeysInput;
import com.linkedin.dataset.DeclaredKey;
import com.linkedin.dataset.DeclaredKeyArray;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.AspectUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Replaces the keys users declare for a dataset. Requires Edit Entity on the dataset, the same
 * privilege REST and OpenAPI writes of the {@code declaredKeys} aspect require. Field paths and key
 * shapes are checked by the aspect validator, so every write API applies the same rules.
 */
@RequiredArgsConstructor
public class UpdateDeclaredKeysResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final DisjunctivePrivilegeGroup EDIT_ENTITY_PRIVILEGES =
      new DisjunctivePrivilegeGroup(
          List.of(
              new ConjunctivePrivilegeGroup(
                  List.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()))));

  private final EntityClient entityClient;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn urn = UrnUtils.getUrn(bindArgument(environment.getArgument("urn"), String.class));
    final DeclaredKeysInput input =
        bindArgument(environment.getArgument("input"), DeclaredKeysInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!Constants.DATASET_ENTITY_NAME.equals(urn.getEntityType())) {
            throw new IllegalArgumentException(
                String.format("Declared keys are only supported on datasets, got %s", urn));
          }
          // Authorize before the existence check so the error does not reveal whether it exists.
          if (!AuthorizationUtils.isAuthorized(
              context, urn.getEntityType(), urn.toString(), EDIT_ENTITY_PRIVILEGES)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          try {
            if (!entityClient.exists(context.getOperationContext(), urn)) {
              throw new IllegalArgumentException(
                  String.format("Failed to update declared keys. %s does not exist.", urn));
            }
            final Aspect existing =
                entityClient.getLatestAspectObject(
                    context.getOperationContext(), urn, Constants.DECLARED_KEYS_ASPECT_NAME, false);
            final AuditStamp now =
                new AuditStamp()
                    .setActor(UrnUtils.getUrn(context.getActorUrn()))
                    .setTime(System.currentTimeMillis());
            final DeclaredKeys keys = new DeclaredKeys().setLastModified(now);
            keys.setCreated(
                existing != null && new DeclaredKeys(existing.data()).hasCreated()
                    ? new DeclaredKeys(existing.data()).getCreated()
                    : now);
            if (input.getPrimaryKey() != null && !input.getPrimaryKey().isEmpty()) {
              keys.setPrimaryKey(new StringArray(input.getPrimaryKey()));
            }
            if (input.getBusinessKeys() != null && !input.getBusinessKeys().isEmpty()) {
              keys.setBusinessKeys(
                  new DeclaredKeyArray(
                      input.getBusinessKeys().stream()
                          .map(
                              key -> {
                                final DeclaredKey declaredKey =
                                    new DeclaredKey()
                                        .setName(key.getName())
                                        .setFields(new StringArray(key.getFields()));
                                if (key.getDescription() != null) {
                                  declaredKey.setDescription(key.getDescription());
                                }
                                return declaredKey;
                              })
                          .collect(Collectors.toList())));
            }
            entityClient.ingestProposal(
                context.getOperationContext(),
                AspectUtils.buildMetadataChangeProposal(
                    urn, Constants.DECLARED_KEYS_ASPECT_NAME, keys),
                false);
            return true;
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update declared keys for %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
