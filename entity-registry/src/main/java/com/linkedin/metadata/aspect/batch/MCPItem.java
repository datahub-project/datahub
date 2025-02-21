package com.linkedin.metadata.aspect.batch;

import com.google.common.collect.ImmutableSet;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.models.AspectSpec;
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
    // Limit initial support to defined templates
    if (!AspectTemplateEngine.SUPPORTED_TEMPLATES.contains(aspectSpec.getName())) {
      // Prevent unexpected behavior for aspects that do not currently have 1st class patch support,
      // specifically having array based fields that require merging without specifying merge
      // behavior can get into bad states
      throw new UnsupportedOperationException(
          "Aspect: " + aspectSpec.getName() + " does not currently support patch " + "operations.");
    }
    return true;
  }
}
