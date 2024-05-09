package com.linkedin.metadata.graph;

import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class RelatedEntitiesResult {
  int start;
  int count;
  int total;
  List<RelatedEntity> entities;
}
