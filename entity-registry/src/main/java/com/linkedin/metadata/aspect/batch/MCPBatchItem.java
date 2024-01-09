package com.linkedin.metadata.aspect.batch;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.mxe.MetadataChangeProposal;
import javax.annotation.Nullable;

/** Represents a proposal to write to the primary data store which may be represented by an MCP */
public abstract class MCPBatchItem implements BatchItem {

  @Nullable
  public abstract MetadataChangeProposal getMetadataChangeProposal();

  /**
   * Validates that a change type is valid for the given aspect
   *
   * @param changeType
   * @param aspectSpec
   * @return
   */
  protected static boolean isValidChangeType(ChangeType changeType, AspectSpec aspectSpec) {
    if (aspectSpec.isTimeseries()) {
      // Timeseries aspects only support UPSERT
      return ChangeType.UPSERT.equals(changeType);
    } else {
      if (ChangeType.PATCH.equals(changeType)) {
        return supportsPatch(aspectSpec);
      } else {
        return ChangeType.UPSERT.equals(changeType);
      }
    }
  }

  protected static boolean supportsPatch(AspectSpec aspectSpec) {
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
