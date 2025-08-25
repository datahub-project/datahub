package com.linkedin.metadata.service;

import static org.testng.Assert.*;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.util.OwnerServiceUtils;
import java.net.URISyntaxException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerServiceUtilsTest {

  public static String TECHNICAL_OWNER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__technical_owner";
  public static String BUSINESS_OWNER_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__business_owner";
  public static String DATA_STEWARD_OWNERSHIP_TYPE_URN =
      "urn:li:ownershipType:__system__data_steward";

  private Urn ownerUrn1;
  private Urn ownerUrn2;
  private Urn technicalOwnershipTypeUrn;
  private Urn businessOwnershipTypeUrn;
  private Urn dataStewardOwnershipTypeUrn;

  @BeforeMethod
  public void setUp() throws URISyntaxException {
    ownerUrn1 = new Urn("urn:li:corpuser:foo");
    ownerUrn2 = new Urn("urn:li:corpuser:bar");
    technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    businessOwnershipTypeUrn = new Urn(BUSINESS_OWNER_OWNERSHIP_TYPE_URN);
    dataStewardOwnershipTypeUrn = new Urn(DATA_STEWARD_OWNERSHIP_TYPE_URN);
  }

  // Tests for addOwnerToAspect method

  @Test
  public void testAddOwnerToAspectWithoutExistingOwners() {
    Ownership ownership = new Ownership();

    OwnerServiceUtils.addOwnerToAspect(
        ownership,
        ownerUrn1,
        OwnershipType.TECHNICAL_OWNER,
        technicalOwnershipTypeUrn,
        OwnershipSourceType.MANUAL);

    assertTrue(ownership.hasOwners());
    assertEquals(1, ownership.getOwners().size());

    Owner addedOwner = ownership.getOwners().get(0);
    assertEquals(ownerUrn1, addedOwner.getOwner());
    assertEquals(OwnershipType.TECHNICAL_OWNER, addedOwner.getType());
    assertEquals(technicalOwnershipTypeUrn, addedOwner.getTypeUrn());
    assertEquals(OwnershipSourceType.MANUAL, addedOwner.getSource().getType());
  }

  @Test
  public void testAddOwnerToAspectWithExistingOwners() {
    Ownership ownership = new Ownership();
    OwnerArray existingOwners = new OwnerArray();

    // Add an existing owner
    Owner existingOwner = new Owner();
    existingOwner.setOwner(ownerUrn2);
    existingOwner.setType(OwnershipType.BUSINESS_OWNER);
    existingOwner.setTypeUrn(businessOwnershipTypeUrn);
    existingOwner.setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL));
    existingOwners.add(existingOwner);

    ownership.setOwners(existingOwners);

    OwnerServiceUtils.addOwnerToAspect(
        ownership, ownerUrn1, OwnershipType.TECHNICAL_OWNER, technicalOwnershipTypeUrn);

    assertEquals(2, ownership.getOwners().size());

    // Verify the new owner was added
    Owner newOwner = ownership.getOwners().get(1);
    assertEquals(ownerUrn1, newOwner.getOwner());
    assertEquals(OwnershipType.TECHNICAL_OWNER, newOwner.getType());
    assertEquals(technicalOwnershipTypeUrn, newOwner.getTypeUrn());
  }

  @Test
  public void testAddOwnerToAspectReplacesExistingOwnerSameTypeUrn() {
    Ownership ownership = new Ownership();
    OwnerArray existingOwners = new OwnerArray();

    // Add an existing owner with same URN and type
    Owner existingOwner = new Owner();
    existingOwner.setOwner(ownerUrn1);
    existingOwner.setType(OwnershipType.TECHNICAL_OWNER);
    existingOwner.setTypeUrn(technicalOwnershipTypeUrn);
    existingOwner.setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL));
    existingOwners.add(existingOwner);

    ownership.setOwners(existingOwners);

    OwnerServiceUtils.addOwnerToAspect(
        ownership, ownerUrn1, OwnershipType.TECHNICAL_OWNER, technicalOwnershipTypeUrn);

    // Should still only have 1 owner (the existing one was replaced)
    assertEquals(1, ownership.getOwners().size());

    Owner updatedOwner = ownership.getOwners().get(0);
    assertEquals(ownerUrn1, updatedOwner.getOwner());
    assertEquals(OwnershipType.TECHNICAL_OWNER, updatedOwner.getType());
    assertEquals(technicalOwnershipTypeUrn, updatedOwner.getTypeUrn());
  }

  @Test
  public void testAddOwnerToAspectWithDifferentOwnershipTypes() {
    Ownership ownership = new Ownership();

    // Add technical owner
    OwnerServiceUtils.addOwnerToAspect(
        ownership, ownerUrn1, OwnershipType.TECHNICAL_OWNER, technicalOwnershipTypeUrn);

    // Add business owner for same user
    OwnerServiceUtils.addOwnerToAspect(
        ownership, ownerUrn1, OwnershipType.BUSINESS_OWNER, businessOwnershipTypeUrn);

    assertEquals(2, ownership.getOwners().size());

    // Both ownership types should be present for the same user
    assertTrue(
        ownership.getOwners().stream()
            .anyMatch(
                owner ->
                    owner.getOwner().equals(ownerUrn1)
                        && owner.getTypeUrn().equals(technicalOwnershipTypeUrn)));
    assertTrue(
        ownership.getOwners().stream()
            .anyMatch(
                owner ->
                    owner.getOwner().equals(ownerUrn1)
                        && owner.getTypeUrn().equals(businessOwnershipTypeUrn)));
  }

  // Tests for removeExistingOwnerIfExists method

  @Test
  public void testRemoveExistingOwnerIfExistsWithMatchingOwner() {
    OwnerArray ownerArray = new OwnerArray();

    // Add multiple owners
    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);
    owner1.setTypeUrn(technicalOwnershipTypeUrn);
    ownerArray.add(owner1);

    Owner owner2 = new Owner();
    owner2.setOwner(ownerUrn2);
    owner2.setType(OwnershipType.BUSINESS_OWNER);
    owner2.setTypeUrn(businessOwnershipTypeUrn);
    ownerArray.add(owner2);

    assertEquals(2, ownerArray.size());

    // Remove owner1
    OwnerServiceUtils.removeExistingOwnerIfExists(ownerArray, ownerUrn1, technicalOwnershipTypeUrn);

    assertEquals(1, ownerArray.size());
    assertEquals(ownerUrn2, ownerArray.get(0).getOwner());
  }

  @Test
  public void testRemoveExistingOwnerIfExistsWithNoMatch() {
    OwnerArray ownerArray = new OwnerArray();

    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);
    owner1.setTypeUrn(technicalOwnershipTypeUrn);
    ownerArray.add(owner1);

    assertEquals(1, ownerArray.size());

    // Try to remove non-existent owner
    OwnerServiceUtils.removeExistingOwnerIfExists(ownerArray, ownerUrn2, businessOwnershipTypeUrn);

    // Should still have 1 owner
    assertEquals(1, ownerArray.size());
    assertEquals(ownerUrn1, ownerArray.get(0).getOwner());
  }

  @Test
  public void testRemoveExistingOwnerIfExistsWithEmptyArray() {
    OwnerArray ownerArray = new OwnerArray();

    assertEquals(0, ownerArray.size());

    // Try to remove from empty array
    OwnerServiceUtils.removeExistingOwnerIfExists(ownerArray, ownerUrn1, technicalOwnershipTypeUrn);

    // Should still be empty
    assertEquals(0, ownerArray.size());
  }

  @Test
  public void testRemoveExistingOwnerIfExistsWithNullTypeUrn() {
    OwnerArray ownerArray = new OwnerArray();

    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);
    ownerArray.add(owner1);

    assertEquals(1, ownerArray.size());

    // Remove with null type URN - should match any owner with matching URN
    OwnerServiceUtils.removeExistingOwnerIfExists(ownerArray, ownerUrn1, null);

    assertEquals(0, ownerArray.size());
  }

  // Tests for mapOwnershipTypeToEntity method

  @Test
  public void testMapOwnershipType() {
    assertEquals(
        OwnerServiceUtils.mapOwnershipTypeToEntity("TECHNICAL_OWNER"),
        UrnUtils.getUrn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN));
  }

  @Test
  public void testMapOwnershipTypeBusinessOwner() {
    assertEquals(
        OwnerServiceUtils.mapOwnershipTypeToEntity("BUSINESS_OWNER"),
        UrnUtils.getUrn(BUSINESS_OWNER_OWNERSHIP_TYPE_URN));
  }

  @Test
  public void testMapOwnershipTypeDataSteward() {
    assertEquals(
        OwnerServiceUtils.mapOwnershipTypeToEntity("DATA_STEWARD"),
        UrnUtils.getUrn(DATA_STEWARD_OWNERSHIP_TYPE_URN));
  }

  @Test
  public void testMapOwnershipTypeLowercase() {
    assertEquals(
        OwnerServiceUtils.mapOwnershipTypeToEntity("technical_owner"),
        UrnUtils.getUrn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN));
  }

  @Test
  public void testMapOwnershipTypeMixedCase() {
    assertEquals(
        OwnerServiceUtils.mapOwnershipTypeToEntity("Technical_Owner"),
        UrnUtils.getUrn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN));
  }

  @Test
  public void testMapOwnershipTypeWithSystemIdConstant() {
    String result = OwnerServiceUtils.mapOwnershipTypeToEntity("CUSTOM_TYPE").toString();
    assertTrue(result.contains(OwnerServiceUtils.SYSTEM_ID));
    assertTrue(result.contains("custom_type"));
  }

  @Test
  public void testMapOwnershipTypeEmptyString() {
    Urn result = OwnerServiceUtils.mapOwnershipTypeToEntity("");
    String expectedUrn = "urn:li:ownershipType:" + OwnerServiceUtils.SYSTEM_ID;
    assertEquals(expectedUrn, result.toString());
  }

  @Test
  public void testMapOwnershipTypeSpecialCharacters() {
    // Test with special characters that get converted to lowercase
    Urn result = OwnerServiceUtils.mapOwnershipTypeToEntity("SPECIAL-TYPE_123");
    String expectedUrn = "urn:li:ownershipType:" + OwnerServiceUtils.SYSTEM_ID + "special-type_123";
    assertEquals(expectedUrn, result.toString());
  }

  @Test
  public void testIsOwnerEqualUrnOnly() throws URISyntaxException {
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    assertTrue(OwnerServiceUtils.isOwnerEqual(owner1, ownerUrn1, null));

    Urn ownerUrn2 = new Urn("urn:li:corpuser:bar");
    assertFalse(OwnerServiceUtils.isOwnerEqual(owner1, ownerUrn2, null));
  }

  @Test
  public void testIsOwnerEqualWithLegacyTypeOnly() throws URISyntaxException {

    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner ownerWithTechnicalOwnership = new Owner();
    ownerWithTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithTechnicalOwnership.setType(OwnershipType.TECHNICAL_OWNER);

    assertTrue(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));

    Owner ownerWithBusinessOwnership = new Owner();
    ownerWithBusinessOwnership.setOwner(ownerUrn1);
    ownerWithBusinessOwnership.setType(OwnershipType.BUSINESS_OWNER);
    assertFalse(
        OwnerServiceUtils.isOwnerEqual(
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
        OwnerServiceUtils.isOwnerEqual(
            ownerWithTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithBusinessOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertTrue(OwnerServiceUtils.isOwnerEqual(ownerWithTechnicalOwnership, ownerUrn1, null));
    assertTrue(OwnerServiceUtils.isOwnerEqual(ownerWithoutOwnershipType, ownerUrn1, null));
    assertFalse(OwnerServiceUtils.isOwnerEqual(owner2WithoutOwnershipType, ownerUrn1, null));
  }

  @Test
  public void testIsOwnerEqualWithBothLegacyAndNewType() throws URISyntaxException {
    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn businessOwnershipTypeUrn = new Urn(BUSINESS_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");

    Owner ownerWithLegacyTechnicalOwnership = new Owner();
    ownerWithLegacyTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithLegacyTechnicalOwnership.setType(OwnershipType.TECHNICAL_OWNER);

    assertTrue(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithLegacyTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithLegacyTechnicalOwnership, ownerUrn1, businessOwnershipTypeUrn));

    Owner ownerWithNewTechnicalOwnership = new Owner();
    ownerWithNewTechnicalOwnership.setOwner(ownerUrn1);
    ownerWithNewTechnicalOwnership.setTypeUrn(technicalOwnershipTypeUrn);

    assertTrue(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithNewTechnicalOwnership, ownerUrn1, technicalOwnershipTypeUrn));
    assertFalse(
        OwnerServiceUtils.isOwnerEqual(
            ownerWithNewTechnicalOwnership, ownerUrn1, businessOwnershipTypeUrn));
  }

  // Additional edge case tests for isOwnerEqual method

  @Test
  public void testIsOwnerEqualWithNullOwnerTypeUrn() {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn1);
    owner.setType(OwnershipType.TECHNICAL_OWNER);
    // No typeUrn set

    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, null));
    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, technicalOwnershipTypeUrn));
  }

  @Test
  public void testIsOwnerEqualWithMismatchedUrns() {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn1);
    owner.setType(OwnershipType.TECHNICAL_OWNER);
    owner.setTypeUrn(technicalOwnershipTypeUrn);

    assertFalse(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn2, technicalOwnershipTypeUrn));
    assertFalse(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn2, null));
  }

  @Test
  public void testIsOwnerEqualWithLegacyTypeMapping() {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn1);
    owner.setType(OwnershipType.BUSINESS_OWNER);
    // No typeUrn set, should fall back to legacy type mapping

    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, businessOwnershipTypeUrn));
    assertFalse(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, technicalOwnershipTypeUrn));
  }

  @Test
  public void testIsOwnerEqualWithDataStewardType() {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn1);
    owner.setType(OwnershipType.DATA_STEWARD);

    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, dataStewardOwnershipTypeUrn));
    assertFalse(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, technicalOwnershipTypeUrn));
  }

  @Test
  public void testIsOwnerEqualWithNoneType() {
    Owner owner = new Owner();
    owner.setOwner(ownerUrn1);
    owner.setType(OwnershipType.NONE);

    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, null));
    // When ownershipTypeUrn is provided but owner has NONE type, it should fall back to mapping
    Urn noneTypeUrn = OwnerServiceUtils.mapOwnershipTypeToEntity("NONE");
    assertTrue(OwnerServiceUtils.isOwnerEqual(owner, ownerUrn1, noneTypeUrn));
  }

  // Tests for SYSTEM_ID constant usage

  @Test
  public void testSystemIdConstant() {
    assertEquals("__system__", OwnerServiceUtils.SYSTEM_ID);
  }

  @Test
  public void testSystemIdUsedInMapping() {
    String customType = "CUSTOM_OWNERSHIP";
    Urn result = OwnerServiceUtils.mapOwnershipTypeToEntity(customType);
    String expected =
        "urn:li:ownershipType:" + OwnerServiceUtils.SYSTEM_ID + customType.toLowerCase();
    assertEquals(expected, result.toString());
  }

  @Test
  public void testSystemIdInAllStandardTypes() {
    String[] standardTypes = {"TECHNICAL_OWNER", "BUSINESS_OWNER", "DATA_STEWARD"};

    for (String type : standardTypes) {
      Urn result = OwnerServiceUtils.mapOwnershipTypeToEntity(type);
      assertTrue(result.toString().contains(OwnerServiceUtils.SYSTEM_ID));
    }
  }

  @Test
  public void testFullOwnershipWorkflowWithTypeUrns() {
    Ownership ownership = new Ownership();

    // Add owner using type URN
    OwnerServiceUtils.addOwnerToAspect(
        ownership, ownerUrn1, OwnershipType.TECHNICAL_OWNER, technicalOwnershipTypeUrn);

    assertEquals(1, ownership.getOwners().size());
    Owner addedOwner = ownership.getOwners().get(0);
    assertEquals(ownerUrn1, addedOwner.getOwner());
    assertEquals(technicalOwnershipTypeUrn, addedOwner.getTypeUrn());

    // Verify exact match
    assertTrue(OwnerServiceUtils.isOwnerEqual(addedOwner, ownerUrn1, technicalOwnershipTypeUrn));

    // Verify removal works
    OwnerServiceUtils.removeExistingOwnerIfExists(
        ownership.getOwners(), ownerUrn1, technicalOwnershipTypeUrn);
    assertEquals(0, ownership.getOwners().size());
  }
}
