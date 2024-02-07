package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.OwnerUpdate;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupUtils;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public class OwnerUpdateMapper implements ModelMapper<OwnerUpdate, Owner> {

  private static final OwnerUpdateMapper INSTANCE = new OwnerUpdateMapper();

  public static Owner map(@Nonnull final OwnerUpdate input) {
    return INSTANCE.apply(input);
  }

  @Override
  public Owner apply(@Nonnull final OwnerUpdate input) {
    final Owner owner = new Owner();
    try {
      if (Urn.createFromString(input.getOwner()).getEntityType().equals("corpuser")) {
        owner.setOwner(CorpUserUtils.getCorpUserUrn(input.getOwner()));
      } else if (Urn.createFromString(input.getOwner()).getEntityType().equals("corpGroup")) {
        owner.setOwner(CorpGroupUtils.getCorpGroupUrn(input.getOwner()));
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    if (input.getOwnershipTypeUrn() != null) {
      owner.setTypeUrn(UrnUtils.getUrn(input.getOwnershipTypeUrn()));
    }
    // For backwards compatibility we have to always set the deprecated type.
    // If the type exists we assume it's an old ownership type that we can map to.
    // Else if it's a net new custom ownership type set old type to CUSTOM.
    OwnershipType type =
        input.getType() != null
            ? OwnershipType.valueOf(input.getType().toString())
            : OwnershipType.CUSTOM;
    owner.setType(type);

    if (input.getOwnershipTypeUrn() != null) {
      owner.setTypeUrn(UrnUtils.getUrn(input.getOwnershipTypeUrn()));
      owner.setType(OwnershipType.CUSTOM);
    }

    owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));
    return owner;
  }
}
