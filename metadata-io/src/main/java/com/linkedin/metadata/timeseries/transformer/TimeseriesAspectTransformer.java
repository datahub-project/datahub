package com.linkedin.metadata.timeseries.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.utils.RecordUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * Class that provides a utility function that transforms the timeseries aspect into a document
 */
@Slf4j
public class TimeseriesAspectTransformer {

  private static String URN_FIELD = "urn";
  private static String TIMESTAMP_FIELD = "timestampMillis";
  private static String EVENT_FIELD = "event";

  private TimeseriesAspectTransformer() {
  }

  public static JsonNode transform(final Urn urn, final RecordTemplate timeseriesAspect) {
    if (!timeseriesAspect.data().containsKey(TIMESTAMP_FIELD)) {
      throw new IllegalArgumentException("Input timeseries aspect does not contain a timestampMillis field");
    }
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put(URN_FIELD, urn.toString());
    document.put(TIMESTAMP_FIELD, (Long) timeseriesAspect.data().get("timestampMillis"));
    document.put(EVENT_FIELD, RecordUtils.toJsonString(timeseriesAspect));
    return document;
  }
}
