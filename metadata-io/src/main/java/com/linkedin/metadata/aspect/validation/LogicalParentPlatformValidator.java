package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INFO_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates that a {@code logicalParent} edge points at a dataset on a logical platform (i.e. a
 * platform whose {@code DataPlatformInfo.logical} is {@code true}). This runs across all write APIs
 * (GraphQL, OpenAPI, RestLI, SDK) so the identity model — physical assets link to logical models,
 * never to other physical assets — is enforced at the aspect layer rather than per-API.
 *
 * <p>Unlink writes (a {@code logicalParent} whose parent edge is cleared) are ignored.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class LogicalParentPlatformValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    // Group items by their parent's platform so the dataPlatformInfo lookups can be batched into a
    // single aspect fetch instead of one query per item.
    final Map<Urn, List<BatchItem>> itemsByPlatform = new HashMap<>();
    mcpItems.forEach(
        item -> {
          final LogicalParent logicalParent = item.getAspect(LogicalParent.class);
          if (logicalParent == null
              || logicalParent.getParent() == null
              || logicalParent.getParent().getDestinationUrn() == null) {
            // Unlink (parent cleared) — nothing to validate.
            return;
          }

          final Urn parentDatasetUrn =
              resolveDatasetUrn(logicalParent.getParent().getDestinationUrn());
          try {
            itemsByPlatform
                .computeIfAbsent(resolvePlatformUrn(parentDatasetUrn), k -> new ArrayList<>())
                .add(item);
          } catch (IllegalArgumentException e) {
            // UrnAnnotationValidator enforces the dataset/schemaField entityTypes constraint, but
            // there is no guaranteed ordering between validators, so a malformed or
            // unexpected-type parent URN must still fail cleanly here rather than throw and take
            // down the whole batch.
            exceptions.addException(item, e.getMessage());
          }
        });

    if (!itemsByPlatform.isEmpty()) {
      final Map<Urn, Map<String, Aspect>> platformAspects =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObjects(
                  operationContext,
                  itemsByPlatform.keySet(),
                  Set.of(DATA_PLATFORM_INFO_ASPECT_NAME));
      itemsByPlatform.forEach(
          (platformUrn, items) -> {
            if (isLogicalPlatform(platformAspects.get(platformUrn))) {
              return;
            }
            items.forEach(
                item ->
                    exceptions.addException(
                        item,
                        String.format(
                            "Logical parent %s must be on a logical platform, but is on platform %s",
                            item.getAspect(LogicalParent.class).getParent().getDestinationUrn(),
                            platformUrn)));
          });
    }

    return exceptions.streamAllExceptions();
  }

  @Nonnull
  private Urn resolveDatasetUrn(@Nonnull final Urn proposedParentUrn) {
    return SchemaFieldUtils.parseSchemaFieldUrn(proposedParentUrn)
        .map(pair -> pair.getFirst())
        .orElse(proposedParentUrn);
  }

  @Nonnull
  private Urn resolvePlatformUrn(@Nonnull final Urn datasetUrn) {
    try {
      return DatasetUrn.createFromUrn(datasetUrn).getPlatformEntity();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Logical parent must resolve to a dataset URN, got: " + datasetUrn, e);
    }
  }

  private boolean isLogicalPlatform(@Nullable final Map<String, Aspect> platformAspects) {
    if (platformAspects == null || !platformAspects.containsKey(DATA_PLATFORM_INFO_ASPECT_NAME)) {
      return false;
    }
    final DataPlatformInfo platformInfo =
        new DataPlatformInfo(platformAspects.get(DATA_PLATFORM_INFO_ASPECT_NAME).data());
    return Boolean.TRUE.equals(platformInfo.isLogical());
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
