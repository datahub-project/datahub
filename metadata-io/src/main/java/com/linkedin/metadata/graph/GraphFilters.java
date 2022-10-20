package com.linkedin.metadata.graph;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class GraphFilters {
  // entity types you want to allow in your result set
  public List<String> allowedEntityTypes;

  public static GraphFilters emptyGraphFilters = new GraphFilters(ImmutableList.of());

  public static GraphFilters defaultGraphFilters = new GraphFilters(ImmutableList.of());
}
