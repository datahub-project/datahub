package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.batch.MCLBatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.entity.ebean.batch.MCLBatchItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.mycompany.dq.DataQualityRuleEvent;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesMCLSideEffect extends MCLSideEffect {

  public CustomDataQualityRulesMCLSideEffect(AspectPluginConfig config) {
    super(config);
  }

  @Override
  protected Stream<MCLBatchItem> applyMCLSideEffect(
      @Nonnull MCLBatchItem input,
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull AspectRetriever aspectRetriever) {

    // Generate Timeseries event aspect based on non-Timeseries aspect
    MetadataChangeLog originMCP = input.getMetadataChangeLog();

    Optional<MCLBatchItem> timeseriesOptional =
        buildEvent(originMCP)
            .map(
                event -> {
                  try {
                    MetadataChangeLog eventMCP = originMCP.clone();
                    eventMCP.setAspect(GenericRecordUtils.serializeAspect(event));
                    eventMCP.setAspectName("customDataQualityRuleEvent");
                    return eventMCP;
                  } catch (CloneNotSupportedException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(
                eventMCP ->
                    MCLBatchItemImpl.builder()
                        .metadataChangeLog(eventMCP)
                        .build(entityRegistry, aspectRetriever));

    return timeseriesOptional.stream();
  }

  private Optional<DataQualityRuleEvent> buildEvent(MetadataChangeLog originMCP) {
    if (originMCP.getAspect() != null) {
      DataQualityRuleEvent event = new DataQualityRuleEvent();
      if (event.getActor() != null) {
        event.setActor(event.getActor());
      }
      event.setEventTimestamp(originMCP.getSystemMetadata().getLastObserved());
      event.setTimestampMillis(originMCP.getSystemMetadata().getLastObserved());
      if (originMCP.getPreviousAspectValue() == null) {
        event.setEventType("RuleCreated");
      } else {
        event.setEventType("RuleUpdated");
      }
      event.setAffectedDataset(originMCP.getEntityUrn());

      return Optional.of(event);
    }

    return Optional.empty();
  }
}
