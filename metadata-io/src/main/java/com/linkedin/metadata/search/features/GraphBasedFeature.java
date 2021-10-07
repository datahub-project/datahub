package com.linkedin.metadata.search.features;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.dao.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.dao.utils.QueryUtils.newRelationshipFilter;


@Slf4j
@RequiredArgsConstructor
public class GraphBasedFeature implements FeatureExtractor {

  private static final Filter EMPTY_FILTER = new Filter().setCriteria(new CriterionArray());

  private final GraphService _graphService;

  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    return ConcurrencyUtils.transformAndCollectAsync(
        entities.stream().map(SearchEntity::getEntity).collect(Collectors.toList()), this::getOutDegree)
        .stream()
        .map(outDegree -> new Features(ImmutableMap.of(Features.Name.OUT_DEGREE, outDegree.doubleValue())))
        .collect(Collectors.toList());
  }

  private int getOutDegree(Urn urn) {
    RelatedEntitiesResult graphResult =
        _graphService.findRelatedEntities("", EMPTY_FILTER, "", newFilter("urn", urn.toString()),
            Collections.emptyList(), newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.OUTGOING), 0, 1000);
    return graphResult.getCount();
  }
}
