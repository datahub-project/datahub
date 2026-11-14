package com.linkedin.metadata.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertSame;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EventProducerPolicyChangeTest {

  private static final Urn POLICY = UrnUtils.getUrn("urn:li:dataHubPolicy:test");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final String TOPIC = "PolicyChange_v1";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EventProducer producer;
  private final Future<?> mclFuture = CompletableFuture.completedFuture(null);

  @BeforeMethod
  public void setup() {
    producer = mock(EventProducer.class, Mockito.withSettings().defaultAnswer(CALLS_REAL_METHODS));
    Mockito.doReturn(mclFuture)
        .when(producer)
        .produceMCL(any(), any(), any(), any(MetadataChangeLog.class));
  }

  @Test
  public void testPolicyChangeIsSentOnlyWhenATopicIsSet() {
    producer.produceMetadataChangeLog(
        opContext, POLICY, mock(AspectSpec.class), new MetadataChangeLog());
    verify(producer, never()).producePolicyChange(any(), any(), any(), any());

    producer.setPolicyChangeTopicName(TOPIC);
    MetadataChangeLog mcl = new MetadataChangeLog();
    producer.produceMetadataChangeLog(opContext, POLICY, mock(AspectSpec.class), mcl);
    verify(producer).producePolicyChange(any(), eq(POLICY), eq(mcl), eq(TOPIC));
  }

  @Test
  public void testOtherEntitiesAreNotSent() {
    producer.setPolicyChangeTopicName(TOPIC);
    producer.produceMetadataChangeLog(
        opContext, DATASET, mock(AspectSpec.class), new MetadataChangeLog());
    verify(producer, never()).producePolicyChange(any(), any(), any(), any());
  }

  @Test
  public void testAPolicyChangeFailureDoesNotAffectTheChangeLogSend() {
    producer.setPolicyChangeTopicName(TOPIC);
    doThrow(new IllegalStateException("broker down"))
        .when(producer)
        .producePolicyChange(any(), any(), any(), any());

    Future<?> result =
        producer.produceMetadataChangeLog(
            opContext, POLICY, mock(AspectSpec.class), new MetadataChangeLog());

    assertSame(result, mclFuture);
  }
}
