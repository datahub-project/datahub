/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.openlineage.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.openlineage.server.OpenLineage;

/** LineageBody */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "schemaURL")
@JsonSubTypes({
  @JsonSubTypes.Type(value = OpenLineage.RunEvent.class, name = LineageBody.RUN_EVENT_SCHEMA),
  @JsonSubTypes.Type(
      value = OpenLineage.DatasetEvent.class,
      name = LineageBody.DATASET_EVENT_SCHEMA),
  @JsonSubTypes.Type(value = OpenLineage.JobEvent.class, name = LineageBody.JOB_EVENT_SCHEMA)
})
public interface LineageBody extends OpenLineage.BaseEvent {
  String RUN_EVENT_SCHEMA =
      "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent";
  String DATASET_EVENT_SCHEMA =
      "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent";
  String JOB_EVENT_SCHEMA =
      "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/JobEvent";
}
