package com.linkedin.datahub.graphql.utils;

import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

public class SystemMetadataUtilsTest {

  private final Long recentLastObserved = 1660056070640L;
  private final Long mediumLastObserved = 1659107340747L;
  private final Long distantLastObserved = 1657226036292L;

  @Test
  public void testGetLastIngested() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("default-run-id", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)
    ));
    aspectMap.put("real-run-id", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId("real-id-1").setLastObserved(mediumLastObserved)
    ));
    aspectMap.put("real-run-id2", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId("real-id-2").setLastObserved(distantLastObserved)
    ));

    Long lastObserved = SystemMetadataUtils.getLastIngested(aspectMap);
    assertEquals(lastObserved, mediumLastObserved);
  }

  @Test
  public void testGetLastIngestedAllDefaultRunIds() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("default-run-id", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(recentLastObserved)
    ));
    aspectMap.put("default-run-id2", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(mediumLastObserved)
    ));
    aspectMap.put("default-run-id3", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId(DEFAULT_RUN_ID).setLastObserved(distantLastObserved)
    ));

    Long lastObserved = SystemMetadataUtils.getLastIngested(aspectMap);
    assertNull(lastObserved, null);
  }

  @Test
  public void testGetLastIngestedNoAspects() {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    Long lastObserved = SystemMetadataUtils.getLastIngested(aspectMap);
    assertNull(lastObserved, null);
  }
}
