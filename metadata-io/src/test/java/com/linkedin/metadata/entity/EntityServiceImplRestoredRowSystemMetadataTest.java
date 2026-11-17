package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.SystemMetadata;
import org.testng.annotations.Test;

public class EntityServiceImplRestoredRowSystemMetadataTest {

  private static final String ACTOR = "urn:li:corpuser:datahub";

  @Test
  public void testLegacySurvivingRowTakesLaterCreationTimeOfDiscardedRow() {
    SystemMetadata surviving = systemMetadata(1_000L, "2");
    SystemMetadata discarded = systemMetadata(5_000L, "4");

    SystemMetadata restored = EntityServiceImpl.restoredRowSystemMetadata(surviving, discarded);

    assertEquals(restored.getVersion(), "5");
    assertEquals(restored.getAspectCreated().getTime().longValue(), 5_000L);
  }

  @Test
  public void testSurvivingRowKeepsItsOwnLaterCreationTime() {
    SystemMetadata surviving = systemMetadata(9_000L, "2");
    SystemMetadata discarded = systemMetadata(5_000L, "4");

    SystemMetadata restored = EntityServiceImpl.restoredRowSystemMetadata(surviving, discarded);

    assertEquals(restored.getVersion(), "5");
    assertEquals(restored.getAspectCreated().getTime().longValue(), 9_000L);
  }

  @Test
  public void testNullSurvivingRowStaysNull() {
    assertNull(EntityServiceImpl.restoredRowSystemMetadata(null, systemMetadata(5_000L, "4")));
  }

  private static SystemMetadata systemMetadata(long createdTime, String version) {
    AuditStamp created = new AuditStamp().setTime(createdTime).setActor(UrnUtils.getUrn(ACTOR));
    return new SystemMetadata().setVersion(version).setAspectCreated(created);
  }
}
