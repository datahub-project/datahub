package com.linkedin.datahub.graphql.types.common.mappers.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Setter
@Getter
@AllArgsConstructor
public class RunInfo {
  private final String id;
  private final Long time;
}
