package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.EntityRelationshipLegacy;
import com.linkedin.datahub.graphql.generated.EntityWithRelationships;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class EntityRelationshipLegacyMapper
    implements ModelMapper<com.linkedin.common.EntityRelationship, EntityRelationshipLegacy> {

  public static final EntityRelationshipLegacyMapper INSTANCE =
      new EntityRelationshipLegacyMapper();

  public static EntityRelationshipLegacy map(
      @Nonnull final com.linkedin.common.EntityRelationship relationship) {
    return INSTANCE.apply(relationship);
  }

  @Override
  public EntityRelationshipLegacy apply(
      @Nonnull final com.linkedin.common.EntityRelationship relationship) {
    final EntityRelationshipLegacy result = new EntityRelationshipLegacy();

    EntityWithRelationships partialLineageEntity =
        (EntityWithRelationships) UrnToEntityMapper.map(relationship.getEntity());
    if (partialLineageEntity != null) {
      result.setEntity(partialLineageEntity);
    }
    if (relationship.hasCreated()) {
      result.setCreated(AuditStampMapper.map(relationship.getCreated()));
    }
    return result;
  }
}
