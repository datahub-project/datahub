package com.linkedin.metadata.aspect.validation;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates that lifecycleStage URNs on Status aspects reference existing lifecycleStageType
 * entities and that the stage is applicable to the target entity's type.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class LifecycleStageValidator extends AspectPayloadValidator {

  private static final String LIFECYCLE_STAGE_TYPE_INFO_ASPECT = "lifecycleStageTypeInfo";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    for (BatchItem item : mcpItems) {
      if (!Constants.STATUS_ASPECT_NAME.equals(item.getAspectName())) {
        continue;
      }

      Status status = item.getAspect(Status.class);
      if (status == null || !status.hasLifecycleStage()) {
        continue;
      }

      Urn stageUrn = status.getLifecycleStage();
      LifecycleStageTypeInfo info = fetchStageInfo(stageUrn, retrieverContext);

      if (info == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "Lifecycle stage '%s' does not exist. "
                        + "Use listLifecycleStages to discover available stages.",
                    stageUrn)));
        continue;
      }

      String entityType = item.getUrn().getEntityType();
      if (info.hasEntityTypes() && !info.getEntityTypes().isEmpty()) {
        List<String> allowedTypes = info.getEntityTypes();
        if (!allowedTypes.contains(entityType)) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "Lifecycle stage '%s' does not apply to entity type '%s'. "
                          + "Allowed entity types: %s",
                      stageUrn, entityType, allowedTypes)));
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  private LifecycleStageTypeInfo fetchStageInfo(Urn stageUrn, RetrieverContext retrieverContext) {
    try {
      RecordTemplate aspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(stageUrn, LIFECYCLE_STAGE_TYPE_INFO_ASPECT);
      if (aspect == null) {
        return null;
      }
      return new LifecycleStageTypeInfo(aspect.data());
    } catch (Exception e) {
      log.warn("Failed to fetch lifecycle stage info for {}", stageUrn, e);
      return null;
    }
  }
}
