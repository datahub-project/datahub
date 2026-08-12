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
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class CoalesceRetentionBufferTest {

  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String ASPECT = "status";
  private static final OperationContext SYSTEM_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  @SuppressWarnings("unchecked")
  public void testEnqueueRoutesThroughResolverThenBuffer() {
    OffloadBuffer<RetentionKey, Long> offloadBuffer = mock(OffloadBuffer.class);
    RetentionContextResolver resolver = new SimpleRetentionContextResolver();
    RetentionKey expectedKey = new SimpleRetentionKey(TEST_URN.toString(), ASPECT);

    CoalesceRetentionBuffer buffer = new CoalesceRetentionBuffer(offloadBuffer, resolver);

    buffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 7L);

    verify(offloadBuffer, times(1)).enqueue(eq(expectedKey), eq(7L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEnqueueRoutesThroughCustomResolverThenBuffer() {
    // Asserts the wiring contract: enqueue delegates to resolver.enrichKey then buffer.enqueue.
    OffloadBuffer<RetentionKey, Long> offloadBuffer = mock(OffloadBuffer.class);
    RetentionContextResolver resolver = mock(RetentionContextResolver.class);
    RetentionKey expectedKey = mock(RetentionKey.class);
    when(resolver.enrichKey(SYSTEM_CONTEXT, TEST_URN, ASPECT)).thenReturn(expectedKey);

    CoalesceRetentionBuffer buffer = new CoalesceRetentionBuffer(offloadBuffer, resolver);

    buffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 7L);

    verify(resolver, times(1)).enrichKey(eq(SYSTEM_CONTEXT), eq(TEST_URN), eq(ASPECT));
    verify(offloadBuffer, times(1)).enqueue(eq(expectedKey), eq(7L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefersApplyIsAlwaysTrue() {
    OffloadBuffer<RetentionKey, Long> offloadBuffer = mock(OffloadBuffer.class);
    CoalesceRetentionBuffer buffer =
        new CoalesceRetentionBuffer(offloadBuffer, new SimpleRetentionContextResolver());

    assertTrue(buffer.defersApply());
  }
}
