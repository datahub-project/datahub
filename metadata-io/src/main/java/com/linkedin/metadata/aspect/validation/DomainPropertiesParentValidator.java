package com.linkedin.metadata.aspect.validation;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.DomainProperties;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Prevents a domain from becoming its own parent across all metadata write APIs. A write that
 * leaves an already-stored self-parent unchanged is allowed, so a name or description edit can
 * round-trip the legacy parent. The entity-graph read path drops that edge.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DomainPropertiesParentValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    // The stored parent is not available until pre-commit. Rejecting here would block a
    // read-modify-write that only changes name or description on a legacy self-parent.
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (ChangeMCP item : changeMCPs) {
      DomainProperties proposed = item.getAspect(DomainProperties.class);
      if (!isSelfParent(item.getUrn(), proposed)
          || isSelfParent(item.getUrn(), previousProperties(item))) {
        continue;
      }
      exceptions.addException(
          AspectValidationException.forItem(item, "A domain cannot be its own parent."));
    }

    return exceptions.streamAllExceptions();
  }

  @Nullable
  private static DomainProperties previousProperties(ChangeMCP item) {
    if (item.getPreviousRecordTemplate() == null) {
      return null;
    }
    return item.getPreviousAspect(DomainProperties.class);
  }

  private static boolean isSelfParent(@Nonnull Urn urn, @Nullable DomainProperties properties) {
    return properties != null
        && properties.hasParentDomain()
        && urn.equals(properties.getParentDomain());
  }
}
