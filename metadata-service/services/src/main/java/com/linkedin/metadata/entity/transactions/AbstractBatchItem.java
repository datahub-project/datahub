package com.linkedin.metadata.entity.transactions;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractBatchItem {
  // urn an urn associated with the new aspect
  public abstract Urn getUrn();

  // aspectName name of the aspect being inserted
  public abstract String getAspectName();

  public abstract SystemMetadata getSystemMetadata();

  public abstract ChangeType getChangeType();

  public abstract EntitySpec getEntitySpec();

  public abstract AspectSpec getAspectSpec();

  public abstract MetadataChangeProposal getMetadataChangeProposal();

  public abstract void validateUrn(EntityRegistry entityRegistry, Urn urn);

  @Nonnull
  protected static SystemMetadata generateSystemMetadataIfEmpty(
      @Nullable SystemMetadata systemMetadata) {
    if (systemMetadata == null) {
      systemMetadata = new SystemMetadata();
      systemMetadata.setRunId(DEFAULT_RUN_ID);
      systemMetadata.setLastObserved(System.currentTimeMillis());
    }
    return systemMetadata;
  }

  protected static AspectSpec validateAspect(MetadataChangeProposal mcp, EntitySpec entitySpec) {
    if (!mcp.hasAspectName() || !mcp.hasAspect()) {
      throw new UnsupportedOperationException(
          "Aspect and aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcp.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Unknown aspect %s for entity %s", mcp.getAspectName(), mcp.getEntityType()));
    }

    return aspectSpec;
  }

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
