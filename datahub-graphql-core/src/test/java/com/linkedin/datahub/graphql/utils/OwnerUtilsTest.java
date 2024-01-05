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
  public void testIsOwnerEqualWithLegacyTypeIsNotConsidered() throws URISyntaxException {

    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    owner1.setType(OwnershipType.TECHNICAL_OWNER);

    assertTrue(OwnerUtils.isOwnerEqual(owner1, ownerUrn1, technicalOwnershipTypeUrn));

    Owner owner2 = new Owner();
    owner2.setOwner(ownerUrn1);
    owner2.setType(OwnershipType.BUSINESS_OWNER);
    assertTrue(
        OwnerUtils.isOwnerEqual(owner1, ownerUrn1, new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN)));
  }

  @Test
  public void testIsOwnerEqualOwnershipTypeUrnIsConsidered() throws URISyntaxException {

    Urn technicalOwnershipTypeUrn = new Urn(TECHNICAL_OWNER_OWNERSHIP_TYPE_URN);
    Urn ownerUrn1 = new Urn("urn:li:corpuser:foo");
    Owner owner1 = new Owner();
    owner1.setOwner(ownerUrn1);
    owner1.setTypeUrn(technicalOwnershipTypeUrn);

    assertTrue(OwnerUtils.isOwnerEqual(owner1, ownerUrn1, technicalOwnershipTypeUrn));

    Urn businessOwnershipTypeUrn = new Urn("urn:li:ownershipType:__system__business_owner");
    Owner owner2 = new Owner();
    owner2.setOwner(ownerUrn1);
    owner2.setTypeUrn(businessOwnershipTypeUrn);
    assertTrue(OwnerUtils.isOwnerEqual(owner1, ownerUrn1, technicalOwnershipTypeUrn));
  }
}
