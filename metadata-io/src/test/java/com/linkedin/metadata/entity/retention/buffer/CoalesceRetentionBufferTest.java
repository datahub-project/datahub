package com.linkedin.metadata.entity.retention.buffer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class CoalesceRetentionBufferTest {

  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String ASPECT = "status";

  @Test
  @SuppressWarnings("unchecked")
  public void testEnqueueRoutesThroughResolverThenBuffer() {
    OffloadBuffer<RetentionKey, Long> offloadBuffer = mock(OffloadBuffer.class);
    RetentionContextResolver<RetentionKey> resolver = mock(RetentionContextResolver.class);
    OperationContext opContext = mock(OperationContext.class);
    RetentionKey expectedKey = new RetentionKey(TEST_URN.toString(), ASPECT);
    when(resolver.enrichKey(opContext, TEST_URN, ASPECT)).thenReturn(expectedKey);

    CoalesceRetentionBuffer buffer = new CoalesceRetentionBuffer(offloadBuffer, resolver);

    buffer.enqueue(opContext, TEST_URN, ASPECT, 7L);

    verify(resolver, times(1)).enrichKey(eq(opContext), eq(TEST_URN), eq(ASPECT));
    verify(offloadBuffer, times(1)).enqueue(eq(expectedKey), eq(7L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefersApplyIsAlwaysTrue() {
    OffloadBuffer<RetentionKey, Long> offloadBuffer = mock(OffloadBuffer.class);
    RetentionContextResolver<RetentionKey> resolver = mock(RetentionContextResolver.class);
    CoalesceRetentionBuffer buffer = new CoalesceRetentionBuffer(offloadBuffer, resolver);

    assertTrue(buffer.defersApply());
  }
}
