package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.FabricType;
import com.linkedin.common.FabricTypeNames;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.ContainerProperties;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.DatasetKey;
import org.testng.annotations.Test;

/**
 * FabricTypeName is a string typeref bound to the FabricType enum in generated Java. Built-in
 * values keep working through the typed accessors; custom values are stored unchanged and read as
 * $UNKNOWN through them.
 */
public class FabricTypeNameBindingTest {

  private static final Urn PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:hive");

  @Test
  public void testBuiltInValueRoundTripsThroughTypedAccessors() {
    DatasetKey key =
        new DatasetKey().setPlatform(PLATFORM).setName("db.t").setOrigin(FabricType.PROD);
    assertEquals(key.data().get("origin"), "PROD");
    assertEquals(key.getOrigin(), FabricType.PROD);
  }

  @Test
  public void testCustomValueIsStoredUnchangedAndReadsAsUnknown() {
    DatasetKey key = new DatasetKey().setPlatform(PLATFORM).setName("db.t");
    FabricTypeNames.set(key, "origin", "STAGING_EU");

    assertEquals(key.getOrigin(), FabricType.$UNKNOWN);
    assertEquals(FabricTypeNames.get(key, "origin"), "STAGING_EU");

    DatasetKey copy = RecordUtils.toRecordTemplate(DatasetKey.class, RecordUtils.toJsonString(key));
    assertEquals(copy, key);
    assertEquals(FabricTypeNames.get(copy, "origin"), "STAGING_EU");
  }

  @Test
  public void testWritingUnknownIsRefused() {
    assertThrows(RuntimeException.class, () -> new DatasetKey().setOrigin(FabricType.$UNKNOWN));
  }

  @Test
  public void testUrnToKeyAndBackPreservesCustomValue() {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,STAGING_EU)");
    RecordTemplate key =
        EntityKeyUtils.convertUrnToEntityKeyInternal(urn, new DatasetKey().schema());
    assertEquals(FabricTypeNames.get(key, "origin"), "STAGING_EU");
    assertEquals(EntityKeyUtils.convertEntityKeyToUrn(key, "dataset"), urn);
  }

  @Test
  public void testOptionalAspectField() {
    ContainerProperties absent = new ContainerProperties().setName("c");
    assertNull(absent.getEnv());
    assertFalse(absent.hasEnv());

    ContainerProperties custom = new ContainerProperties().setName("c");
    FabricTypeNames.set(custom, "env", "STAGING_EU");
    assertTrue(custom.hasEnv());
    assertEquals(custom.getEnv(), FabricType.$UNKNOWN);

    ContainerProperties builtIn = new ContainerProperties().setName("c").setEnv(FabricType.DEV);
    assertEquals(builtIn.data().get("env"), "DEV");
    assertEquals(builtIn.getEnv(), FabricType.DEV);
  }
}
