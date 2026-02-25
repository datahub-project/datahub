package datahub.client.v2.patch;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.config.ServerConfig;
import datahub.client.v2.operations.EntityClient;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Transforms patch MCPs based on server version compatibility.
 *
 * <p>Handles known version-specific issues with patches on DataHub servers &lt;= v1.3.0. For
 * incompatible servers, transforms patches to full aspect replacements using read-modify-write.
 *
 * <p>Version thresholds:
 *
 * <ul>
 *   <li>Domain patches (domains): DataHub Core > v1.3.0, DataHub Cloud TBD
 *   <li>Container properties patches (editableContainerProperties): DataHub Core > v1.3.0, DataHub
 *       Cloud TBD
 *   <li>Dataset properties patches (editableDatasetProperties): DataHub Core > v1.3.0, DataHub
 *       Cloud TBD
 *   <li>ML Model Group properties patches (editableMLModelGroupProperties): DataHub Core > v1.3.0,
 *       DataHub Cloud TBD
 * </ul>
 *
 * <p>Note: Tag patches (globalTags) do not require transformation as GlobalTagsTemplate has always
 * existed on servers. Historical tag patch issues were due to client-side bugs in
 * GlobalTagsPatchBuilder (using "urn" instead of "tag" field), which have been fixed.
 */
@Slf4j
@RequiredArgsConstructor
public class VersionAwarePatchTransformer implements PatchTransformer {

  @Nonnull private final EntityClient entityClient;

  /**
   * Transforms patches based on server version.
   *
   * @param patches the original patch MCPs
   * @param serverConfig the server configuration (null if not fetched)
   * @return transformed list of MCPs
   * @throws Exception if transformation fails
   */
  @Override
  @Nonnull
  public List<TransformResult> transform(
      @Nonnull List<MetadataChangeProposal> patches, @Nullable ServerConfig serverConfig)
      throws Exception {

    log.debug("VersionAwarePatchTransformer.transform called with {} patches", patches.size());

    List<TransformResult> results = new ArrayList<>();

    if (serverConfig == null) {
      log.warn(
          "Server config not available - cannot perform version-aware transformation. Passing"
              + " patches through unchanged.");
      // Return patches with null retry functions (not retryable)
      for (MetadataChangeProposal patch : patches) {
        results.add(new TransformResult(patch, null));
      }
      return results;
    }

    log.debug(
        "Server config: version={}, isCore={}", serverConfig.getVersion(), serverConfig.isCore());

    boolean needsTransformation = needsPatchTransformation(serverConfig);
    log.debug("needsTransformation={}", needsTransformation);

    for (MetadataChangeProposal patch : patches) {
      String aspectName = patch.getAspectName();
      log.debug("Processing patch for aspect: {}, entityUrn: {}", aspectName, patch.getEntityUrn());

      // mlModelProperties patches are not supported on ANY current server version
      // Always transform to full aspect replacement with retry function
      if ("mlModelProperties".equals(aspectName)) {
        log.info(
            "Transforming mlModelProperties patch to full aspect replacement with optimistic"
                + " locking for urn: {}",
            patch.getEntityUrn());

        MetadataChangeProposal fullMcp =
            datahub.client.v2.entity.MLModel.transformPatchToFullAspect(patch, entityClient);

        // Create retry function as closure that captures patch and entityClient
        java.util.function.Function<
                datahub.client.v2.exceptions.VersionConflictInfo, MetadataChangeProposal>
            retryFn =
                (conflict) -> {
                  try {
                    log.warn(
                        "Version conflict on mlModelProperties: expected={}, actual={}."
                            + " Retrying with fresh read...",
                        conflict.getExpectedVersion(),
                        conflict.getActualVersion());
                    // Re-transform with fresh read to get latest version
                    return datahub.client.v2.entity.MLModel.transformPatchToFullAspect(
                        patch, entityClient);
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Failed to retry transform for mlModelProperties patch", e);
                  }
                };

        results.add(new TransformResult(fullMcp, retryFn));
        log.debug("Added mlModelProperties TransformResult with retry function");
        continue;
      }

      // Check if this server version needs transformation for other aspects
      if (!needsTransformation) {
        // Server supports all other patches - no transformation needed, no retry function
        results.add(new TransformResult(patch, null));
        continue;
      }

      // Check each known problematic patch type (for old server versions)
      // Note: domains now use UPSERT directly, not patches, so no transformation needed
      // TODO: Add retry functions for these once their transformPatchToFullAspect methods are
      // updated
      if ("editableContainerProperties".equals(aspectName)) {
        log.info(
            "Server version {} does not support editableContainerProperties patches - transforming"
                + " to full aspect replacement",
            serverConfig.getVersion());
        MetadataChangeProposal fullMcp =
            datahub.client.v2.entity.Container.transformPatchToFullAspect(patch, entityClient);
        results.add(new TransformResult(fullMcp, null)); // No retry function yet
      } else if ("editableDatasetProperties".equals(aspectName)) {
        log.info(
            "Server version {} does not support editableDatasetProperties patches - transforming"
                + " to full aspect replacement",
            serverConfig.getVersion());
        MetadataChangeProposal fullMcp =
            datahub.client.v2.entity.Dataset.transformPatchToFullAspect(patch, entityClient);
        results.add(new TransformResult(fullMcp, null)); // No retry function yet
      } else if ("editableMLModelGroupProperties".equals(aspectName)) {
        log.info(
            "Server version {} does not support editableMLModelGroupProperties patches -"
                + " transforming to full aspect replacement",
            serverConfig.getVersion());
        MetadataChangeProposal fullMcp =
            datahub.client.v2.entity.MLModelGroup.transformPatchToFullAspect(patch, entityClient);
        results.add(new TransformResult(fullMcp, null)); // No retry function yet
      } else {
        // Not a known problematic patch - pass through without retry function
        results.add(new TransformResult(patch, null));
      }
    }

    return results;
  }

  /**
   * Checks if the server version requires patch transformation.
   *
   * <p>The 4 problematic patches (domains, editableContainerProperties, editableDatasetProperties,
   * editableMLModelGroupProperties) are missing on the same server versions:
   *
   * <ul>
   *   <li>DataHub Core <= v1.3.0
   *   <li>DataHub Cloud: TBD (need to determine threshold)
   * </ul>
   *
   * @param serverConfig the server configuration
   * @return true if patches need transformation
   */
  private boolean needsPatchTransformation(@Nonnull ServerConfig serverConfig) {
    if (serverConfig.isCore()) {
      // Core versions <= v1.3.0 need transformation
      return !serverConfig.isVersionAtLeast(1, 3, 1);
    } else {
      // TODO: Determine Cloud version threshold for patch support
      // For now, assume Cloud supports patches (conservative approach)
      return false;
    }
  }
}
