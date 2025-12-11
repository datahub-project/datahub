/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
