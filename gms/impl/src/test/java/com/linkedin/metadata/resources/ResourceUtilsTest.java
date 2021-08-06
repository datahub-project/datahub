package com.linkedin.metadata.resources;

import com.linkedin.common.BrowsePath;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import org.testng.annotations.Test;
import org.testng.Assert;


public class ResourceUtilsTest {
  @Test
  public void testValidateOrThrowThrowsOnMissingUnrecognizedField() {
    DataMap rawMap = new DataMap();
    rawMap.put("removed", true);
    rawMap.put("extraField", 1);
    Status status = new Status(rawMap);
    Assert.assertThrows(RestLiServiceException.class, () -> ResourceUtils.validateOrThrow(status, HttpStatus.S_500_INTERNAL_SERVER_ERROR));
  }

  @Test
  public void testValidateOrThrowThrowsOnMissingRequiredField() {
    DataMap rawMap = new DataMap();
    BrowsePath status = new BrowsePath(rawMap);
    Assert.assertThrows(RestLiServiceException.class, () -> ResourceUtils.validateOrThrow(status, HttpStatus.S_500_INTERNAL_SERVER_ERROR));
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingOptionalField() throws Exception {
    DataMap rawMap = new DataMap();
    Owner owner = new Owner(rawMap);
    owner.setOwner(Urn.createFromString("urn:li:corpuser:test"));
    owner.setType(OwnershipType.DATAOWNER);
    ResourceUtils.validateOrThrow(owner, HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingDefaultField() {
    DataMap rawMap = new DataMap();
    Status status = new Status(rawMap);
    ResourceUtils.validateOrThrow(status, HttpStatus.S_500_INTERNAL_SERVER_ERROR);
  }
}
