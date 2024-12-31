package com.linkedin.metadata.aspect.models.graph;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class RelatedEntitiesScrollResult {
  int numResults;
  int pageSize;
  String scrollId;
  List<RelatedEntities> entities;
}
