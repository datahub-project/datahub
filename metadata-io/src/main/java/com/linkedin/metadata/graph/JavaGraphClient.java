package com.linkedin.metadata.graph;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;


@Slf4j
public class JavaGraphClient implements GraphClient {

  GraphService _graphService;
  public JavaGraphClient(@Nonnull GraphService graphService) {
    this._graphService = graphService;
  }

  /**
   * Returns a list of related entities for a given entity, set of edge types, and direction relative to the
   * source node
   */
  @Nonnull
  @Override
  public EntityRelationships getRelatedEntities(String rawUrn, List<String> relationshipTypes,
    RelationshipDirection direction, @Nullable Integer start, @Nullable Integer count, String actor) {

    start = start == null ? 0 : start;
    count = count == null ? DEFAULT_PAGE_SIZE : count;

    RelatedEntitiesResult relatedEntitiesResult =
        _graphService.findRelatedEntities(null,
            QueryUtils.newFilter("urn", rawUrn),
            null,
            EMPTY_FILTER,
            relationshipTypes,
            QueryUtils.newRelationshipFilter(EMPTY_FILTER, direction),
            start,
            count
        );

    final EntityRelationshipArray entityArray = new EntityRelationshipArray(
        relatedEntitiesResult.getEntities().stream().map(
            entity -> {
              try {
                return new EntityRelationship()
                    .setEntity(Urn.createFromString(entity.getUrn()))
                    .setType(entity.getRelationshipType());
              } catch (URISyntaxException e) {
                throw new RuntimeException(
                    String.format("Failed to convert urnStr %s found in the Graph to an Urn object", entity.getUrn()));
              }
            }
        ).collect(Collectors.toList())
    );

    return new EntityRelationships()
        .setStart(relatedEntitiesResult.getStart())
        .setCount(relatedEntitiesResult.getCount())
        .setTotal(relatedEntitiesResult.getTotal())
        .setRelationships(entityArray);
  }

  /**
   * Returns lineage relationships for given entity in the DataHub graph.
   * Lineage relationship denotes whether an entity is directly upstream or downstream of another entity
   */
  @Nonnull
  @Override
  public EntityLineageResult getLineageEntities(String rawUrn, LineageDirection direction, @Nullable Integer start,
      @Nullable Integer count, int maxHops, String actor) {
    return _graphService.getLineage(UrnUtils.getUrn(rawUrn), direction, start != null ? start : 0,
        count != null ? count : 100, maxHops);
  }
}
