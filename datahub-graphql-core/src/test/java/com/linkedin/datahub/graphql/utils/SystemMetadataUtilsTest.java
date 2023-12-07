package com.linkedin.datahub.graphql.utils;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.types.common.mappers.util.RunInfo;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;
import java.util.List;
import org.testng.annotations.Test;

public class SystemMetadataUtilsTest {

  private final Long recentLastObserved = 1660056070640L;
  private final Long mediumLastObserved = 1659107340747L;
  private final Long distantLastObserved = 1657226036292L;

  @Test
  public void testGetLastIngestedTime() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        "default-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)));
    aspectMap.put(
        "real-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-1").setLastObserved(mediumLastObserved)));
    aspectMap.put(
        "real-run-id2",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-2").setLastObserved(distantLastObserved)));

    Long lastObserved = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    assertEquals(lastObserved, mediumLastObserved);
  }

  @Test
  public void testGetLastIngestedRunId() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        "default-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)));
    aspectMap.put(
        "real-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-1").setLastObserved(mediumLastObserved)));
    aspectMap.put(
        "real-run-id2",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-2").setLastObserved(distantLastObserved)));

    String lastRunId = SystemMetadataUtils.getLastIngestedRunId(aspectMap);
    assertEquals(lastRunId, "real-id-1");
  }

  @Test
  public void testGetLastIngestedRuns() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        "default-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)));
    aspectMap.put(
        "real-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-1").setLastObserved(mediumLastObserved)));
    aspectMap.put(
        "real-run-id2",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-2").setLastObserved(distantLastObserved)));

    List<RunInfo> runs = SystemMetadataUtils.getLastIngestionRuns(aspectMap);

    assertEquals(runs.size(), 2);
    assertEquals(runs.get(0), new RunInfo("real-id-1", mediumLastObserved));
    assertEquals(runs.get(1), new RunInfo("real-id-2", distantLastObserved));
  }

  @Test
  public void testGetLastIngestedTimeAllDefaultRunIds() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        "default-run-id",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)));
    aspectMap.put(
        "default-run-id2",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(mediumLastObserved)));
    aspectMap.put(
        "default-run-id3",
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata()
                    .setRunId(DEFAULT_RUN_ID)
                    .setLastObserved(distantLastObserved)));

    Long lastObserved = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    assertNull(lastObserved, null);
  }

  @Test
  public void testGetLastIngestedNoAspects() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    Long lastObserved = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    assertNull(lastObserved, null);
  }
}
