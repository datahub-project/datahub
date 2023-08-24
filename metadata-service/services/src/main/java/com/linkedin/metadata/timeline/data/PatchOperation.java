package com.linkedin.metadata.timeline.data;

import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class PatchOperation {
  String op;
  String path;
  String value;
}
