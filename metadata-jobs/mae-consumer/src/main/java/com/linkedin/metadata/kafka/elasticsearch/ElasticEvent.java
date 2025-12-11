/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.elasticsearch;

import com.linkedin.events.metadata.ChangeType;
import lombok.Data;
import org.opensearch.core.xcontent.XContentBuilder;

@Data
public abstract class ElasticEvent {

  private String index;
  private String type;
  private String id;
  private ChangeType actionType;

  public XContentBuilder buildJson() {
    return null;
  }
}
