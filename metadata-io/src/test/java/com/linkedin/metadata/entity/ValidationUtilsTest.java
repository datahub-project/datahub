package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.BrowsePath;
import com.linkedin.common.FabricType;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ValidationUtilsTest {
  private EntityRegistry entityRegistry;

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  }

  @Test
  public void testValidateOrThrowThrowsOnMissingUnrecognizedField() {
    DataMap rawMap = new DataMap();
    rawMap.put("removed", true);
    rawMap.put("extraField", 1);
    Status status = new Status(rawMap);
    assertThrows(ValidationException.class, () -> ValidationApiUtils.validateOrThrow(status));
  }

  @Test
  public void testValidateOrThrowThrowsOnMissingRequiredField() {
    DataMap rawMap = new DataMap();
    BrowsePath status = new BrowsePath(rawMap);
    assertThrows(ValidationException.class, () -> ValidationApiUtils.validateOrThrow(status));
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingOptionalField() throws Exception {
    DataMap rawMap = new DataMap();
    Owner owner = new Owner(rawMap);
    owner.setOwner(Urn.createFromString("urn:li:corpuser:test"));
    owner.setType(OwnershipType.DATAOWNER);
    ValidationApiUtils.validateOrThrow(owner);
  }

  @Test
  public void testValidateOrThrowDoesNotThrowOnMissingDefaultField() {
    DataMap rawMap = new DataMap();
    Status status = new Status(rawMap);
    ValidationApiUtils.validateOrThrow(status);
  }

  @Test
  public void testConvertEntityUrnToKeyUrlEncoded() throws URISyntaxException {
    final Urn urn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");

    ValidationApiUtils.validateUrn(entityRegistry, urn);

    final AspectSpec keyAspectSpec =
        entityRegistry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    final RecordTemplate actualKey = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec);

    final DatasetKey expectedKey = new DatasetKey();
    expectedKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:s3"));
    expectedKey.setName(
        "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    expectedKey.setOrigin(FabricType.PROD);

    assertEquals(actualKey, expectedKey);

    final Urn invalidUrn =
        Urn.createFromString(
            "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    assertThrows(
        IllegalArgumentException.class,
        () -> ValidationApiUtils.validateUrn(entityRegistry, invalidUrn));
  }
}
