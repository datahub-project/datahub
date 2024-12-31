package com.linkedin.metadata.utils;

import static org.testng.Assert.*;

import com.datahub.test.KeyPartEnum;
import com.datahub.test.TestEntityKey;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntityKeyUtils} */
public class EntityKeyUtilsTest {

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
  public void testConvertEntityKeyToUrn() throws Exception {
    final TestEntityKey key = new TestEntityKey();
    key.setKeyPart1("part1");
    key.setKeyPart2(Urn.createFromString("urn:li:testEntity2:part2"));
    key.setKeyPart3(KeyPartEnum.VALUE_1);

    final Urn expectedUrn =
        Urn.createFromString("urn:li:testEntity1:(part1,urn:li:testEntity2:part2,VALUE_1)");
    final Urn actualUrn = EntityKeyUtils.convertEntityKeyToUrn(key, "testEntity1");
    assertEquals(actualUrn.toString(), expectedUrn.toString());
  }

  @Test
  public void testConvertEntityKeyToUrnInternal() throws Exception {
    final Urn urn =
        Urn.createFromString("urn:li:testEntity1:(part1,urn:li:testEntity2:part2,VALUE_1)");
    final TestEntityKey expectedKey = new TestEntityKey();
    expectedKey.setKeyPart1("part1");
    expectedKey.setKeyPart2(Urn.createFromString("urn:li:testEntity2:part2"));
    expectedKey.setKeyPart3(KeyPartEnum.VALUE_1);

    final RecordTemplate actualKey =
        EntityKeyUtils.convertUrnToEntityKeyInternal(urn, expectedKey.schema());
    Assert.assertEquals(actualKey.data(), expectedKey.data());
  }

  @Test
  public void testConvertEntityUrnToKey() throws Exception {
    final Urn urn =
        Urn.createFromString("urn:li:testEntity:(part1,urn:li:testEntity:part2,VALUE_1)");
    final TestEntityKey expectedKey = new TestEntityKey();
    expectedKey.setKeyPart1("part1");
    expectedKey.setKeyPart2(Urn.createFromString("urn:li:testEntity:part2"));
    expectedKey.setKeyPart3(KeyPartEnum.VALUE_1);

    ConfigEntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            TestEntityKey.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(PegasusUtils.urnToEntityName(urn));

    final RecordTemplate actualKey =
        EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec());
    Assert.assertEquals(actualKey.data(), expectedKey.data());
  }

  @Test
  public void testConvertEntityUrnToKeyUrlEncoded() throws URISyntaxException {
    final Urn urn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");
    final AspectSpec keyAspectSpec =
        entityRegistry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    final RecordTemplate actualKey = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec);

    final DatasetKey expectedKey = new DatasetKey();
    expectedKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:s3"));
    expectedKey.setName(
        "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    expectedKey.setOrigin(FabricType.PROD);

    assertEquals(actualKey, expectedKey);
  }
}
