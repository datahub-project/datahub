package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.OwnershipTypeEntity;
import com.linkedin.datahub.graphql.types.common.mappers.util.KnownEntities;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import com.linkedin.metadata.utils.UnknownDataGuard;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class OwnerMapper {
  private static final UnknownDataGuard UNKNOWN_OWNER =
      UnknownDataGuard.forSite(OwnerMapper.class, "owner");

  public static final OwnerMapper INSTANCE = new OwnerMapper();

  @Nullable
  public static Owner map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.Owner owner,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, owner, entityUrn);
  }

  @Nullable
  public Owner apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.common.Owner owner,
      @Nonnull final Urn entityUrn) {
    if (!KnownEntities.isKnown(context, owner.getOwner())) {
      // An owner of an entity type the registry doesn't know (e.g. added by a newer version before
      // a rollback) can't be represented; skip it rather than nulling every owner of the asset.
      UNKNOWN_OWNER.skipped(
          KnownEntities.metrics(context), owner.getOwner().getEntityType(), null, owner.getOwner());
      return null;
    }
    final Owner result = new Owner();
    // Deprecated. An ownership type only a newer version knows falls back to CUSTOM.
    final OwnershipType ownershipType =
        PdlEnumMapper.map(OwnershipType.class, owner.getType(), OwnershipType.CUSTOM);
    result.setType(ownershipType);

    if (owner.getTypeUrn() == null) {
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
      result.setSource(OwnershipSourceMapper.map(context, owner.getSource()));
    }
    if (owner.getAttribution() != null) {
      result.setAttribution(MetadataAttributionMapper.map(context, owner.getAttribution()));
    }
    result.setAssociatedUrn(entityUrn.toString());
    return result;
  }
}
