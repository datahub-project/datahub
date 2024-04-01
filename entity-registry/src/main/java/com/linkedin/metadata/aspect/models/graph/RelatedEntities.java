package com.linkedin.metadata.aspect.models.graph;

import com.linkedin.metadata.query.filter.RelationshipDirection;
import javax.annotation.Nonnull;
import lombok.Getter;

/** Preserves directionality as well as the generic `related` urn concept */
@Getter
public class RelatedEntities extends RelatedEntity {
  /** source Urn * */
  @Nonnull String sourceUrn;

  /** Destination Urn associated with the related entity. */
  @Nonnull String destinationUrn;

  public RelatedEntities(
      @Nonnull String relationshipType,
      @Nonnull String sourceUrn,
      @Nonnull String destinationUrn,
      @Nonnull RelationshipDirection relationshipDirection,
      String viaEntity) {
    super(
        relationshipType,
        relationshipDirection == RelationshipDirection.OUTGOING ? destinationUrn : sourceUrn,
        viaEntity);
    this.sourceUrn = sourceUrn;
    this.destinationUrn = destinationUrn;
  }

  public RelatedEntity asRelatedEntity() {
    return new RelatedEntity(relationshipType, urn, via);
  }
}
