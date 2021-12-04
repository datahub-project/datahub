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


public class RetentionStoreTest {
  @Test
  public void testStore() {
    RetentionStore retentionStore = new RetentionStore(new SnapshotEntityRegistry(new Snapshot()),
        RetentionStoreTest.class.getResourceAsStream("/test-retention.yaml"));
    assertEquals(retentionStore.getRetentionForEntity("testEntity"),
        ImmutableList.of(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(10)),
            new Retention().setTime(new TimeBasedRetention().setMaxAgeInSeconds(20))));
    assertEquals(retentionStore.getRetentionForEntity("random"),
        ImmutableList.of(new Retention().setIndefinite(new IndefiniteRetention())));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidation() {
    new RetentionStore(new SnapshotEntityRegistry(new Snapshot()),
        RetentionStoreTest.class.getResourceAsStream("/invalid-retention.yaml"));
  }
}
