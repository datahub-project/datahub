package com.linkedin.metadata.authorization;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class OwnershipUtilsTest {

  private static final Urn USER = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn OTHER_USER = UrnUtils.getUrn("urn:li:corpuser:other");
  private static final Urn GROUP = UrnUtils.getUrn("urn:li:corpGroup:testGroup");
  private static final Urn TECHNICAL_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");
  private static final Urn BUSINESS_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__business_owner");

  @Test
  public void noOwnershipTypeFilterMatchesAnyOwner() {
    Ownership ownership = ownershipOf(USER, TECHNICAL_OWNER);

    assertTrue(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), /* ownershipTypes */ null));
    assertTrue(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), Collections.emptyList()));
  }

  @Test
  public void ownershipTypeFilterRestrictsToMatchingType() {
    Ownership ownership = ownershipOf(USER, TECHNICAL_OWNER);

    assertTrue(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), List.of(TECHNICAL_OWNER)));
    assertFalse(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), List.of(BUSINESS_OWNER)));
  }

  @Test
  public void groupOwnershipIsRespected() {
    Ownership ownership = ownershipOf(GROUP, TECHNICAL_OWNER);

    assertTrue(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, List.of(GROUP), List.of(TECHNICAL_OWNER)));
    assertFalse(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, List.of(GROUP), List.of(BUSINESS_OWNER)));
  }

  @Test
  public void nonOwnerReturnsFalse() {
    Ownership ownership = ownershipOf(OTHER_USER, TECHNICAL_OWNER);

    assertFalse(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), List.of(TECHNICAL_OWNER)));
  }

  @Test
  public void enumOnlyOwnerIsMatchedByDerivedSystemTypeUrn() {
    Ownership ownership = enumOnlyOwnershipOf(USER, OwnershipType.TECHNICAL_OWNER);

    assertTrue(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), List.of(TECHNICAL_OWNER)));
    assertFalse(
        OwnershipUtils.isOwnerOfEntityWithType(
            ownership, USER, Collections.emptyList(), List.of(BUSINESS_OWNER)));
  }

  private static Ownership ownershipOf(Urn ownerUrn, Urn ownershipTypeUrn) {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn);
    owner.setTypeUrn(ownershipTypeUrn);

    OwnerArray owners = new OwnerArray();
    owners.add(owner);

    Ownership ownership = new Ownership();
    ownership.setOwners(owners);
    return ownership;
  }

  private static Ownership enumOnlyOwnershipOf(Urn ownerUrn, OwnershipType type) {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn);
    owner.setType(type);

    OwnerArray owners = new OwnerArray();
    owners.add(owner);

    Ownership ownership = new Ownership();
    ownership.setOwners(owners);
    return ownership;
  }
}
