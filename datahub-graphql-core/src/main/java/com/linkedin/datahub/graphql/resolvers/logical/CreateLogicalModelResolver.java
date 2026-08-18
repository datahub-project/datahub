package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateLogicalModelInput;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Creates a logical model (a dataset on a logical platform). */
@Slf4j
@RequiredArgsConstructor
public class CreateLogicalModelResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String LOGICAL_PLATFORM = "logical";
  // DataPlatformInfo.name has a 15-character limit (see DataPlatformInfo.pdl).
  private static final int PLATFORM_NAME_MAX_LENGTH = 15;

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final CreateLogicalModelInput input =
        bindArgument(environment.getArgument("input"), CreateLogicalModelInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateLogicalModels(context)) {
            throw new AuthorizationException(
                "Unauthorized to create logical models. Please contact your DataHub administrator.");
          }
          try {
            if (input.getName() == null || input.getName().trim().isEmpty()) {
              throw new IllegalArgumentException("Logical model name must not be empty.");
            }
            SchemaMetadataUtils.validateColumns(input.getColumns());

            final DataPlatformUrn platformUrn =
                input.getPlatform() != null
                    ? DataPlatformUrn.createFromString(input.getPlatform())
                    : new DataPlatformUrn(LOGICAL_PLATFORM);

            final DatasetKey key = new DatasetKey();
            key.setPlatform(platformUrn);
            key.setName(input.getName());
            key.setOrigin(
                input.getEnv() != null
                    ? FabricType.valueOf(input.getEnv().toString())
                    : FabricType.PROD);

            final Urn datasetUrn = EntityKeyUtils.convertEntityKeyToUrn(key, DATASET_ENTITY_NAME);
            if (_entityClient.exists(context.getOperationContext(), datasetUrn)) {
              throw new IllegalArgumentException(
                  String.format("A logical model already exists at %s.", datasetUrn));
            }

            final List<MetadataChangeProposal> proposals = new ArrayList<>();
            resolvePlatformProposal(context, platformUrn).ifPresent(proposals::add);

            final DatasetProperties properties = new DatasetProperties();
            properties.setName(
                input.getDisplayName() != null ? input.getDisplayName() : input.getName());
            final MetadataChangeProposal propertiesProposal =
                buildMetadataChangeProposalWithKey(
                    key, DATASET_ENTITY_NAME, DATASET_PROPERTIES_ASPECT_NAME, properties);
            propertiesProposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
            proposals.add(propertiesProposal);

            final SchemaMetadata schema =
                SchemaMetadataUtils.buildSchemaMetadata(
                    input.getName(), platformUrn, input.getColumns());
            final MetadataChangeProposal schemaProposal =
                buildMetadataChangeProposalWithUrn(datasetUrn, SCHEMA_METADATA_ASPECT_NAME, schema);
            proposals.add(schemaProposal);

            // Ingest in a single batch so a partial failure can't leave a dataset without its
            // schema, or a new platform without its DataPlatformInfo, either of which would leave
            // the model unidentifiable as logical.
            _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);

            return datasetUrn.toString();
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to create logical model {}", input.getName(), e);
            throw new RuntimeException(
                String.format("Failed to create logical model %s", input.getName()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Resolves the platform for a new logical model, enforcing that it is a logical platform.
   *
   * <p>Returns a {@code DataPlatformInfo} proposal if the platform doesn't exist yet (a new custom
   * logical platform typed in by the user), so it gets created alongside the model. If the platform
   * already exists, it must already be logical — this GraphQL mutation is a programmatic path
   * independent of the UI, which only ever offers logical platforms, so this is a trust-boundary
   * check against a caller passing e.g. {@code snowflake}.
   */
  @Nonnull
  private Optional<MetadataChangeProposal> resolvePlatformProposal(
      @Nonnull final QueryContext context, @Nonnull final DataPlatformUrn platformUrn)
      throws Exception {
    final EntityResponse response =
        _entityClient.getV2(
            context.getOperationContext(),
            DATA_PLATFORM_ENTITY_NAME,
            platformUrn,
            ImmutableSet.of(DATA_PLATFORM_INFO_ASPECT_NAME));

    if (response != null && response.getAspects().containsKey(DATA_PLATFORM_INFO_ASPECT_NAME)) {
      final DataPlatformInfo existing =
          new DataPlatformInfo(
              response.getAspects().get(DATA_PLATFORM_INFO_ASPECT_NAME).getValue().data());
      if (!Boolean.TRUE.equals(existing.isLogical())) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot create a logical model on platform %s: it is not a logical platform.",
                platformUrn));
      }
      return Optional.empty();
    }

    final String platformName = platformUrn.getPlatformNameEntity();
    if (platformName.length() > PLATFORM_NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "Platform name '%s' exceeds the maximum length of %d characters.",
              platformName, PLATFORM_NAME_MAX_LENGTH));
    }

    final DataPlatformInfo newPlatformInfo =
        new DataPlatformInfo()
            .setName(platformName)
            .setDisplayName(platformName)
            .setType(PlatformType.OTHERS)
            .setDatasetNameDelimiter(".")
            .setLogical(true);
    return Optional.of(
        buildMetadataChangeProposalWithUrn(
            platformUrn, DATA_PLATFORM_INFO_ASPECT_NAME, newPlatformInfo));
  }
}
