package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import lombok.AllArgsConstructor;
import lombok.Value;

/** Thin wrapper for an aspect value which is used within the Entity Change Event API. */
@Value
@AllArgsConstructor
public class Aspect<T extends RecordTemplate> {
  /** The aspect value itself. */
  T value;

  /** System metadata */
  SystemMetadata systemMetadata;
}
