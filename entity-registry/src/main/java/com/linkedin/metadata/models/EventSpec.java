package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.EventAnnotation;

/** A specification of a DataHub Platform Event */
public interface EventSpec {
  /** Returns the name of an event */
  String getName();

  /** Returns the raw event annotation */
  EventAnnotation getEventAnnotation();

  /** Returns the PDL schema object for the Event */
  RecordDataSchema getPegasusSchema();
}
