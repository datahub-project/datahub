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
}
