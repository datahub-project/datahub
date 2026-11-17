package com.linkedin.metadata.aspect.batch;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Represents a proposal to write to the primary data store which may be represented by an MCP */
public interface MCPItem extends BatchItem {

  Set<ChangeType> CHANGE_TYPES =
      ImmutableSet.of(
          ChangeType.UPSERT, ChangeType.UPDATE, ChangeType.CREATE, ChangeType.CREATE_ENTITY);

  String HTTP_HEADER_IF_MATCH = "If-Match";

  /**
   * {@code If-Match: *}: the aspect row must already exist. When it does not, the item is dropped
   * as a successful no-op instead of creating the aspect, the entity key aspect, or other defaults.
   */
  String IF_MATCH_ANY = "*";

  @Nullable
  MetadataChangeProposal getMetadataChangeProposal();

  /**
   * Set system metadata on the item
   *
   * @param systemMetadata
   */
  default void setSystemMetadata(@Nonnull SystemMetadata systemMetadata) {
    getMetadataChangeProposal().setSystemMetadata(systemMetadata);
  }

  @Nonnull
  default Map<String, String> getHeaders() {
    if (getMetadataChangeProposal() != null && getMetadataChangeProposal().getHeaders() != null) {
      return getMetadataChangeProposal().getHeaders();
    }
    return Collections.emptyMap();
  }

  default boolean hasHeader(@Nonnull String headerName) {
    return getHeaders().keySet().stream().anyMatch(hdr -> hdr.equalsIgnoreCase(headerName));
  }

  default Optional<String> getHeader(@Nonnull String headerName) {
    return getHeaders().entrySet().stream()
        .filter(entry -> entry.getKey().equalsIgnoreCase(headerName))
        .map(Map.Entry::getValue)
        .findAny();
  }

  /** True when this item carries {@code If-Match: *} and must not create a missing aspect. */
  default boolean requiresExistingAspect() {
    return getHeader(HTTP_HEADER_IF_MATCH)
        .map(value -> IF_MATCH_ANY.equals(value.trim()))
        .orElse(false);
  }

  /**
   * Validates that a change type is valid for the given aspect
   *
   * @param changeType
   * @param aspectSpec
   * @return
   */
  static boolean isValidChangeType(ChangeType changeType, AspectSpec aspectSpec) {
    if (aspectSpec.isTimeseries()) {
      // Timeseries aspects only support UPSERT
      return ChangeType.UPSERT.equals(changeType);
    } else {
      if (ChangeType.PATCH.equals(changeType)) {
        return supportsPatch(aspectSpec);
      } else {
        return CHANGE_TYPES.contains(changeType);
      }
    }
  }

  static boolean supportsPatch(AspectSpec aspectSpec) {
    // All aspects now support patch operations using generic patching.
    // The system will automatically choose between template-based patching (for aspects with
    // templates)
    // and generic patching (for aspects without templates or when forceGenericPatch is enabled).
    return true;
  }

  default void validate(Urn urn, String aspectName, EntityRegistry entityRegistry) {}
}
