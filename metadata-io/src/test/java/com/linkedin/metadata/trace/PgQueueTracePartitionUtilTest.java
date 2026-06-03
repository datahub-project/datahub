package com.linkedin.metadata.trace;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.UrnUtils;
import org.testng.annotations.Test;

public class PgQueueTracePartitionUtilTest {

  @Test
  public void partitionForUrn_stableForSameInputs() {
    var urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:kafka,PageViewEvent,PROD)");
    int p8 = PgQueueTracePartitionUtil.partitionForUrn(urn, 8);
    assertEquals(PgQueueTracePartitionUtil.partitionForUrn(urn, 8), p8);
    assertEquals(PgQueueTracePartitionUtil.partitionForKey(urn.toString(), 8), p8);
  }

  @Test
  public void partitionForUrn_respectsPartitionCount() {
    var urn = UrnUtils.getUrn("urn:li:container:123");
    int p1 = PgQueueTracePartitionUtil.partitionForUrn(urn, 1);
    assertEquals(p1, 0);
  }
}
