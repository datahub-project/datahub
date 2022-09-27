package com.linkedin.metadata.entity;

import com.linkedin.common.BrowsePath;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.entity.validation.ValidationUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ValidationUtilsTest {
  @Test
  public void testValidateOrThrowThrowsOnMissingUnrecognizedField() {
    DataMap rawMap = new DataMap();
    rawMap.put("removed", true);
    rawMap.put("extraField", 1);
    Status status = new Status(rawMap);
    Assert.assertThrows(ValidationException.class, () -> ValidationUtils.validateOrThrow(status));
  }

  @Test
  public void testValidateOrThrowThrowsOnMissingRequiredField() {
    DataMap rawMap = new DataMap();
    BrowsePath status = new BrowsePath(rawMap);
    Assert.assertThrows(ValidationException.class, () -> ValidationUtils.validateOrThrow(status));
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingOptionalField() throws Exception {
    DataMap rawMap = new DataMap();
    Owner owner = new Owner(rawMap);
    owner.setOwner(Urn.createFromString("urn:li:corpuser:test"));
    owner.setType(OwnershipType.DATAOWNER);
    ValidationUtils.validateOrThrow(owner);
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingDefaultField() {
    DataMap rawMap = new DataMap();
    Status status = new Status(rawMap);
    ValidationUtils.validateOrThrow(status);
  }
}
