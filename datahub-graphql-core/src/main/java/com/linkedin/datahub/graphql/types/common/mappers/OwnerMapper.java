package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class OwnerMapper {

  public static final OwnerMapper INSTANCE = new OwnerMapper();

  public static Owner map(
      @Nonnull final com.linkedin.common.Owner owner, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(owner, entityUrn);
  }

  public Owner apply(@Nonnull final com.linkedin.common.Owner owner, @Nonnull final Urn entityUrn) {
    final Owner result = new Owner();
    // Deprecated
    result.setType(Enum.valueOf(OwnershipType.class, owner.getType().toString()));

    if (owner.getTypeUrn() == null) {
      OwnershipType ownershipType = OwnershipType.valueOf(owner.getType().toString());
      owner.setTypeUrn(UrnUtils.getUrn(mapOwnershipTypeToEntity(ownershipType.name())));
    }

    if (owner.getTypeUrn() != null) {
      OwnershipTypeEntity entity = new OwnershipTypeEntity();
      entity.setType(EntityType.CUSTOM_OWNERSHIP_TYPE);
      entity.setUrn(owner.getTypeUrn().toString());
      result.setOwnershipType(entity);
    }
    if (owner.getOwner().getEntityType().equals("corpuser")) {
      CorpUser partialOwner = new CorpUser();
      partialOwner.setUrn(owner.getOwner().toString());
      result.setOwner(partialOwner);
    } else {
      CorpGroup partialOwner = new CorpGroup();
      partialOwner.setUrn(owner.getOwner().toString());
      result.setOwner(partialOwner);
    }
    if (owner.hasSource()) {
      result.setSource(OwnershipSourceMapper.map(owner.getSource()));
    }
    result.setAssociatedUrn(entityUrn.toString());
    return result;
  }
}
