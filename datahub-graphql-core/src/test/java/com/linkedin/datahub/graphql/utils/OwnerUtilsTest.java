package com.linkedin.datahub.graphql.utils;

import static org.testng.AssertJUnit.*;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.resolvers.mutate.util.OwnerUtils;
import java.net.URISyntaxException;
import org.testng.annotations.Test;

public class OwnerUtilsTest {

  public static String TECHNICAL_OWNER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__technical_owner";
  public static String BUSINESS_OWNER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__business_owner";

  @Test
  public void testMapOwnershipType() {
    assertEquals(
        OwnerUtils.mapOwnershipTypeToEntity("TECHNICAL_OWNER"), TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
  }

  @Test
  public void testIsOwnerEqualUrnOnly() throws URISyntaxException {
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    assertTrue(OwnerUtils.isOwnerEqual(owner1, ownerUrn1, null));

    Urn ownerUrn2 = new Urn("urn:li:corpuser:bar");
    assertFalse(OwnerUtils.isOwnerEqual(owner1, ownerUrn2, null));
  }

  @Test
  public void testIsOwnerEqualWithLegacyTypeOnly() throws URISyntaxException {

    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner ownerWithTechnicalOwnership = new Owner();
    ownerWithTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithTechnicalOwnership.setType(OwnershipType.TECHNICAL_OWNER);

    assertTrue(
        OwnerUtils.isOwnerEqual(ownerWithTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));

    Owner ownerWithBusinessOwnership = new Owner();
    ownerWithBusinessOwnership.setOwner(ownerUrn1);
    ownerWithBusinessOwnership.setType(OwnershipType.BUSINESS_OWNER);
    assertFalse(
        OwnerUtils.isOwnerEqual(
            ownerWithBusinessOwnership, ownerUrn1, new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN)));
  }

  @Test
  public void testIsOwnerEqualOnlyOwnershipTypeUrn() throws URISyntaxException {

    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn businessOwnershipTypeUrn = new Urn(BUSINESS_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Urn ownerUrn2 = new Urn("urn:li:corpuser:bar");

    Owner ownerWithTechnicalOwnership = new Owner();
    ownerWithTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithTechnicalOwnership.setTypeUrn(technicalOwnershipTypeUrn);

    Owner ownerWithBusinessOwnership = new Owner();
    ownerWithBusinessOwnership.setOwner(ownerUrn1);
    ownerWithBusinessOwnership.setTypeUrn(businessOwnershipTypeUrn);

    Owner ownerWithoutOwnershipType = new Owner();
    ownerWithoutOwnershipType.setOwner(ownerUrn1);
    ownerWithoutOwnershipType.setType(OwnershipType.NONE);

    Owner owner2WithoutOwnershipType = new Owner();
    owner2WithoutOwnershipType.setOwner(ownerUrn2);
    owner2WithoutOwnershipType.setType(OwnershipType.NONE);

    assertTrue(
        OwnerUtils.isOwnerEqual(ownerWithTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerUtils.isOwnerEqual(ownerWithBusinessOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertTrue(OwnerUtils.isOwnerEqual(ownerWithTechnicalOwnership, ownerUrn1, null));
    assertTrue(OwnerUtils.isOwnerEqual(ownerWithoutOwnershipType, ownerUrn1, null));
    assertFalse(OwnerUtils.isOwnerEqual(owner2WithoutOwnershipType, ownerUrn1, null));
  }

  public void testIsOwnerEqualWithBothLegacyAndNewType() throws URISyntaxException {
    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn businessOwnershipTypeUrn = new Urn(BUSINESS_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");

    Owner ownerWithLegacyTechnicalOwnership = new Owner();
    ownerWithLegacyTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithLegacyTechnicalOwnership.setType(OwnershipType.TECHNICAL_OWNER);

    assertTrue(
        OwnerUtils.isOwnerEqual(
            ownerWithLegacyTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerUtils.isOwnerEqual(
            ownerWithLegacyTechnicalOwnership, ownerUrn1, businessOwnershipTypeUrn));

    Owner ownerWithNewTechnicalOwnership = new Owner();
    ownerWithLegacyTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithLegacyTechnicalOwnership.setTypeUrn(technicalOwnershipTypeUrn);

    assertTrue(
        OwnerUtils.isOwnerEqual(
            ownerWithNewTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerUtils.isOwnerEqual(
            ownerWithNewTechnicalOwnership, ownerUrn1, businessOwnershipTypeUrn));
  }
}
