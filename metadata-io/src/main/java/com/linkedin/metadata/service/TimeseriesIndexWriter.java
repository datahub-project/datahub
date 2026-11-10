package com.linkedin.metadata.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Writes a timeseries aspect into its dedicated Elasticsearch index. That index is independent of
 * the entity search index version, so Search V2 and Search V3 both use this writer.
 */
@Slf4j
public final class TimeseriesIndexWriter {

  private TimeseriesIndexWriter() {}

  public static void upsert(
      @Nonnull OperationContext opContext,
      @Nonnull TimeseriesAspectService timeseriesAspectService,
      @Nonnull String idHashAlgo,
      @Nonnull MCLItem event) {
    AspectSpec aspectSpec = event.getAspectSpec();
    if (!aspectSpec.isTimeseries()) {
      return;
    }

    Urn urn = event.getUrn();
    String entityType = event.getEntitySpec().getName();
    String aspectName = event.getAspectName();
    RecordTemplate aspect = event.getRecordTemplate();
    SystemMetadata systemMetadata = event.getSystemMetadata();

    Map<String, JsonNode> documents;
    try {
      documents =
          TimeseriesAspectTransformer.transform(
              urn, aspect, aspectSpec, systemMetadata, idHashAlgo);
    } catch (JsonProcessingException e) {
      log.error("Failed to generate timeseries document from aspect: {}", e.toString());
      return;
    }

    documents.forEach(
        (docId, document) ->
            timeseriesAspectService.upsertDocument(
                opContext, entityType, aspectName, docId, document));
  }
}
