package datahub.client.v2.patch;

import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.config.ServerConfig;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transforms patch MCPs based on server capabilities and configuration.
 *
 * <p>Allows adaptation of patch operations to work with servers that have different feature support
 * levels. For example, transforming incremental patches to full aspect replacements for servers
 * with broken patch implementations.
 *
 * <p>Transformers are applied at emission time in EntityClient.upsert(), after all patch operations
 * have been accumulated but before they are sent to the server.
 *
 * <p>Each transformed patch is wrapped in a {@link TransformResult} which includes an optional
 * retry function. This enables optimistic locking with retry on version conflicts.
 *
 * <p>Example use case: mlModelProperties patches use read-modify-write and need optimistic locking.
 * The VersionAwarePatchTransformer transforms these to full aspect replacements with
 * "If-Version-Match" headers, and provides retry functions to handle version conflicts.
 */
public interface PatchTransformer {

  /**
   * Transforms a list of patch MCPs based on server configuration.
   *
   * <p>The transformer can:
   *
   * <ul>
   *   <li>Return patches unchanged if server supports them (with null retry function)
   *   <li>Transform patches to full aspect replacements for incompatible servers
   *   <li>Add retry functions for MCPs that use read-modify-write (enabling optimistic locking)
   *   <li>Filter out unsupported patches
   *   <li>Add additional operations based on server capabilities
   * </ul>
   *
   * @param patches the original patch MCPs to transform
   * @param serverConfig the server configuration (null if not fetched or fetch failed)
   * @return list of TransformResult, each containing an MCP and optional retry function
   * @throws Exception if transformation fails (e.g., aspect fetch failure)
   */
  @Nonnull
  List<TransformResult> transform(
      @Nonnull List<MetadataChangeProposal> patches, @Nullable ServerConfig serverConfig)
      throws Exception;
}
