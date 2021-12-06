package com.linkedin.metadata.entity;

import com.datahub.test.Snapshot;
import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.retention.IndefiniteRetention;
import com.linkedin.metadata.retention.Retention;
import com.linkedin.metadata.retention.TimeBasedRetention;
import com.linkedin.metadata.retention.VersionBasedRetention;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class LocalRetentionServiceTest {
  @Test
  public void testStore() {
    LocalRetentionService retentionStore = new LocalRetentionService(new SnapshotEntityRegistry(new Snapshot()),
        LocalRetentionServiceTest.class.getResourceAsStream("/test-retention.yaml"));
    assertEquals(retentionStore.getRetention("testEntity", "testEntityInfo"),
        ImmutableList.of(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(10)),
            new Retention().setTime(new TimeBasedRetention().setMaxAgeInSeconds(20))));
    assertEquals(retentionStore.getRetention("random", "testEntityInfo"),
        ImmutableList.of(new Retention().setIndefinite(new IndefiniteRetention())));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidation() {
    new LocalRetentionService(new SnapshotEntityRegistry(new Snapshot()),
        LocalRetentionServiceTest.class.getResourceAsStream("/invalid-retention.yaml"));
  }
}
