package com.linkedin.datahub.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.models.view.DatasetOwner;
import com.linkedin.identity.CorpUser;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;


public class OwnerUtil {

  private OwnerUtil() {

  }

  /**
   * Convert from TMS Owner to WhereHows DatasetOwner
   * @param owner Owner
   * @return DatasetOwner
   */
  @VisibleForTesting
  public static DatasetOwner toWhOwner(@Nonnull Owner owner, @Nonnull CorpUser corpUser) {
    DatasetOwner dsOwner = new DatasetOwner();
    dsOwner.setConfirmedBy("UI");
    dsOwner.setEmail(corpUser.getInfo().getEmail());
    dsOwner.setIdType("USER");
    dsOwner.setIsActive(corpUser.getInfo().isActive());
    dsOwner.setIsGroup(false);
    dsOwner.setName(corpUser.getInfo().getFullName());
    dsOwner.setNamespace("urn:li:corpuser");
    dsOwner.setSource("UI");
    dsOwner.setType(OWNER_CATEGORY_MAP_INV.get(owner.getType()));
    dsOwner.setUserName(corpUser.getUsername());

    return dsOwner;
  }

  /**
   * Convert from a WhereHows DatasetOwner to TMS Owner
   * @param dsOwner dsOwner
   * @return Owner
   */
  @Nonnull
  public static Owner toTmsOwner(@Nonnull DatasetOwner dsOwner) throws URISyntaxException {
    return new Owner().setOwner(new Urn(dsOwner.getNamespace() + ":" + dsOwner.getUserName()))
        .setType(OWNER_CATEGORY_MAP.get(dsOwner.getType()));
  }

  /**
   * Mapping between WhereHows owner type values and TMS OwnerCategory
   */
  private static final BiMap<String, OwnershipType> OWNER_CATEGORY_MAP =
          new ImmutableBiMap.Builder<String, OwnershipType>()
                  // format
                  .put("DataOwner", OwnershipType.DATAOWNER)
                  .put("Producer", OwnershipType.PRODUCER)
                  .put("Delegate", OwnershipType.DELEGATE)
                  .put("Stakeholder", OwnershipType.STAKEHOLDER)
                  .put("Consumer", OwnershipType.CONSUMER)
                  .put("Developer", OwnershipType.DEVELOPER)
                  .build();

  private static final BiMap<OwnershipType, String> OWNER_CATEGORY_MAP_INV = OWNER_CATEGORY_MAP.inverse();
}
