package com.linkedin.metadata.graph;

import com.linkedin.data.schema.PathSpec;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EdgeMetadata {
  private String aspectName;
  private PathSpec pathSpec;
}
