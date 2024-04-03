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
