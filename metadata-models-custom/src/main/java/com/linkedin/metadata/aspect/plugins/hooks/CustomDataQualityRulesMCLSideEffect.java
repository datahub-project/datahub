package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.mycompany.dq.DataQualityRuleEvent;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class CustomDataQualityRulesMCLSideEffect extends MCLSideEffect {

  private AspectPluginConfig config;

  @Override
  protected Stream<MCLItem> applyMCLSideEffect(
      @Nonnull Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return mclItems.stream()
        .map(
            item -> {
              // Generate Timeseries event aspect based on non-Timeseries aspect
              MetadataChangeLog originMCP = item.getMetadataChangeLog();

              return buildEvent(originMCP)
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
                          MCLItemImpl.builder()
                              .metadataChangeLog(eventMCP)
                              .build(retrieverContext.getAspectRetriever()));
            })
        .filter(Optional::isPresent)
        .map(Optional::get);
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

  @Nonnull
  @Override
  public AspectPluginConfig getConfig() {
    return config;
  }

  @Override
  public CustomDataQualityRulesMCLSideEffect setConfig(@Nonnull AspectPluginConfig config) {
    this.config = config;
    return this;
  }
}
