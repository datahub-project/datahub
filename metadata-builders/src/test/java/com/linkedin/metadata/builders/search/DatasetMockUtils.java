package com.linkedin.metadata.builders.search;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.aspect.DatasetAspect;
import java.util.Arrays;
import javax.annotation.Nonnull;


public class DatasetMockUtils {

  private DatasetMockUtils() {
    // Util class should not have public constructor
  }

  @Nonnull
  public static DatasetAspect ownershipAspect() {
    final DatasetAspect aspect = new DatasetAspect();
    final Owner owner1 = new Owner().setOwner(new CorpuserUrn("foo")).setType(OwnershipType.DEVELOPER);
    final Owner owner2 = new Owner().setOwner(new CorpuserUrn("bar")).setType(OwnershipType.DEVELOPER);
    aspect.setOwnership(new Ownership().setOwners(new OwnerArray(Arrays.asList(owner1, owner2))));
    return aspect;
  }

  @Nonnull
  public static DatasetAspect statusAspect() {
    final DatasetAspect aspect = new DatasetAspect();
    aspect.setStatus(new Status().setRemoved(true));
    return aspect;
  }

  @Nonnull
  public static DatasetAspect datasetPropertiesAspect(@Nonnull String description) {
    final DatasetAspect aspect = new DatasetAspect();
    aspect.setDatasetProperties(new DatasetProperties().setDescription(description));
    return aspect;
  }

  @Nonnull
  public static DatasetAspect deprecationAspect(@Nonnull boolean isDeprecated) {
    final DatasetAspect aspect = new DatasetAspect();
    aspect.setDatasetDeprecation(new DatasetDeprecation().setDeprecated(isDeprecated).setActor(new CorpuserUrn("testUser")).setNote("test purposes"));
    return aspect;
  }

}