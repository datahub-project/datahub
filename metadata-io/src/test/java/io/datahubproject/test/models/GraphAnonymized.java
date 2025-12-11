/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.test.models;

import com.fasterxml.jackson.annotation.JsonSetter;

public class GraphAnonymized {
  public GraphNode source;
  public GraphNode destination;
  public String relationshipType;

  public static class GraphNode extends Anonymized {
    public String urn;
    public String entityType;

    @JsonSetter("urn")
    public void setUrn(String urn) {
      this.urn = anonymizeUrn(urn);
    }
  }
}
