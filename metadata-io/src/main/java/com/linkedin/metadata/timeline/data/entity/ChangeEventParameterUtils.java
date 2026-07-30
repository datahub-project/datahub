package com.linkedin.metadata.timeline.data.entity;

import com.linkedin.common.MetadataAttribution;
import io.datahubproject.metadata.context.ObjectMapperContext;
import javax.annotation.Nullable;

/** Helpers for building ChangeEvent parameter maps shared across entity change events. */
public class ChangeEventParameterUtils {
  private static final String EMPTY_JSON = "{}";

  private ChangeEventParameterUtils() {}

  /**
   * Serialize a MetadataAttribution's sourceDetail map to a JSON string, so propagation consumers
   * can read attribution details (origin, direction, depth, ...) off the emitted ECE. Returns "{}"
   * when attribution is absent.
   */
  public static String serializeSourceDetail(@Nullable final MetadataAttribution attribution) {
    if (attribution == null) {
      return EMPTY_JSON;
    }
    return ObjectMapperContext.defaultMapper.valueToTree(attribution.getSourceDetail()).toString();
  }
}
