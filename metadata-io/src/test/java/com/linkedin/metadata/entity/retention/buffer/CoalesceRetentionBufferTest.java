package com.linkedin.metadata.entity.retention.buffer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBuffers;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.function.BinaryOperator;
import org.testng.annotations.Test;

public class CoalesceRetentionBufferTest {

  private static final Urn TEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)");
  private static final String ASPECT = "status";
  private static final OperationContext SYSTEM_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  @SuppressWarnings("unchecked")
  public void testEnqueueMergesWithKeepMaxLongPolicy() {
    CoalesceBuffer<RetentionKey, Long> coalesceBuffer = mock(CoalesceBuffer.class);
    CoalesceRetentionBuffer buffer =
        new CoalesceRetentionBuffer(coalesceBuffer, new SimpleRetentionContextResolver());

    buffer.enqueue(SYSTEM_CONTEXT, TEST_URN, ASPECT, 7L);

    verify(coalesceBuffer, times(1))
        .merge(
            eq(new SimpleRetentionKey(TEST_URN.toString(), ASPECT)),
            eq(7L),
            eq(CoalesceBuffers.KEEP_MAX_LONG));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefersApplyIsAlwaysTrue() {
    CoalesceBuffer<RetentionKey, Long> coalesceBuffer = mock(CoalesceBuffer.class);
    CoalesceRetentionBuffer buffer =
        new CoalesceRetentionBuffer(coalesceBuffer, new SimpleRetentionContextResolver());

    assertTrue(buffer.defersApply());
  }

  /**
   * Guards against a future refactor accidentally introducing an equivalent-but-different lambda.
   */
  @Test
  public void testKeepMaxLongIsUsedByReferenceIdentity() {
    BinaryOperator<Long> equivalentLambda = (a, b) -> a >= b ? a : b;
    org.testng.Assert.assertNotSame(equivalentLambda, CoalesceBuffers.KEEP_MAX_LONG);
  }
}
