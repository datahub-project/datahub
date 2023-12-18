package com.linkedin.metadata.utils;

import static org.testng.Assert.*;

import com.datahub.test.KeyPartEnum;
import com.datahub.test.TestEntityKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntityKeyUtils} */
public class EntityKeyUtilsTest {

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
}
