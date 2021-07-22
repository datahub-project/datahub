package com.linkedin.metadata.timeseries.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that provides a utility function that transforms the timeseries aspect into a document
 */
@Slf4j
public class TimeseriesAspectTransformer {

  private TimeseriesAspectTransformer() {
  }

  public static JsonNode transform(@Nonnull final Urn urn, @Nonnull final RecordTemplate timeseriesAspect,
      @Nullable final SystemMetadata systemMetadata) {
    if (!timeseriesAspect.data().containsKey(MappingsBuilder.TIMESTAMP_FIELD)) {
      throw new IllegalArgumentException("Input timeseries aspect does not contain a timestampMillis field");
    }
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put(MappingsBuilder.URN_FIELD, urn.toString());
    document.put(MappingsBuilder.TIMESTAMP_FIELD, (Long) timeseriesAspect.data().get("timestampMillis"));
    document.put(MappingsBuilder.EVENT_FIELD, RecordUtils.toJsonString(timeseriesAspect));
    if (systemMetadata != null) {
      document.put(MappingsBuilder.SYSTEM_METADATA_FIELD, RecordUtils.toJsonString(systemMetadata));
    }
    return document;
  }
}
