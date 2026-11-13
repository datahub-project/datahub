package com.linkedin.metadata.config.graphql;

import lombok.Data;

@Data
public class GraphQLQueryConfiguration {
  private int complexityLimit;
  private int depthLimit;
  private boolean introspectionEnabled;

  /**
   * Max depth when traversing parent chains (glossary nodes, domains, containers, documents).
   * Prevents stack overflow and unbounded work in cyclic or deep hierarchies. Default in
   * application.yaml.
   */
  private int maxParentDepth;

  /**
   * Max distinct URNs to visit per GraphQL request during relationship resolution. Beyond this we
   * short-circuit to prevent OOM with large or cyclic graphs. Default in application.yaml.
   */
  private int maxVisitedUrns;

  /**
   * Max datasets one {@code siblingGroups} field may ask for. 0, the default, turns the query off.
   * Each dataset's group walk reads at most 1,000 members, and with view authorization on every
   * member is view-checked. Default in application.yaml.
   */
  private int maxSiblingGroupUrns;

  /** Requests taking longer than this threshold (in ms) are logged at INFO. Default 1000ms. */
  private long slowQueryThresholdMs;

  /**
   * Stream the JSON response to the socket instead of buffering it as one String (bounds GMS heap
   * on large responses). Kill switch; default {@code false} (buffered). Default in
   * application.yaml.
   */
  private boolean streamResponse;
}
