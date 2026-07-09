package com.linkedin.metadata.usage.instrumentation;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.usage.UsageDimensions;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.store.UsageAggregationStore;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.AttributionType;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageMetricsSessionEnricherTest {

  private UsageMetricsSessionEnricher enricher;
  private AtomicInteger recordRequestCount;
  private AtomicInteger recordResponseCount;

  @BeforeMethod
  public void setup() {
    UsageRequestState.clear();
    recordRequestCount = new AtomicInteger();
    recordResponseCount = new AtomicInteger();
    UsageAggregationStore store =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            recordRequestCount.incrementAndGet();
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            recordResponseCount.incrementAndGet();
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    enricher = new UsageMetricsSessionEnricher(store, true);
  }

  @AfterMethod
  public void tearDown() {
    UsageRequestState.clear();
  }

  @Test
  public void testEnrichBeforeBuildDerivesAttributionFromAgentClass() {
    RequestContext.RequestContextBuilder builder =
        openapiBuilder().usageOperation(UsageOperation.METADATA_WRITE.key());
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");

    enricher.enrichBeforeBuild(builder, authentication);

    Assert.assertEquals(builder.peekUsageOperation(), UsageOperation.METADATA_WRITE.key());
    RequestContext requestContext = builder.build();
    Assert.assertEquals(requestContext.getUsageIdentity(), UsageTestFixtures.REGULAR_CORP_USER_URN);
    Assert.assertEquals(requestContext.getAuthChannel(), AuthChannel.SESSION);
    Assert.assertEquals(
        UsageDimensions.resolveAttribution(requestContext), AttributionType.UNKNOWN);
  }

  @Test
  public void testEnrichBeforeBuildSkipsWhenUsageOperationUnset() {
    RequestContext.RequestContextBuilder builder = openapiBuilder();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");

    enricher.enrichBeforeBuild(builder, authentication);

    Assert.assertNull(builder.peekUsageOperation());
  }

  @Test
  public void testOnSessionReadySkipsWhenUsageOperationUnset() throws Exception {
    OperationContext sessionContext = session(null);
    enricher.onSessionReady(sessionContext);
    Assert.assertEquals(recordRequestCount.get(), 0);
  }

  @Test
  public void testOnSessionReadySkipsWhenUsageIdentityUnset() throws Exception {
    RequestContext requestContext =
        openapiBuilder().usageOperation(UsageOperation.METADATA_WRITE.key()).build();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");
    OperationContext sessionContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .requestContext(requestContext)
            .build(authentication, true);

    enricher.onSessionReady(sessionContext);

    Assert.assertEquals(recordRequestCount.get(), 0);
  }

  @Test
  public void testOnSessionReadyRecordsWhenUsageOperationSet() throws Exception {
    OperationContext sessionContext = session(UsageOperation.METADATA_WRITE.key());
    enricher.onSessionReady(sessionContext);
    Assert.assertEquals(recordRequestCount.get(), 1);
  }

  @Test
  public void testCompleteResponseSkipsWhenRecordRequestDidNotRecord() throws Exception {
    UsageAggregationStore noOpStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return false;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            recordResponseCount.incrementAndGet();
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher localEnricher = new UsageMetricsSessionEnricher(noOpStore, true);

    OperationContext sessionContext = session(UsageOperation.METADATA_WRITE.key());
    localEnricher.onSessionReady(sessionContext);
    localEnricher.completeResponse(100L);

    Assert.assertEquals(recordResponseCount.get(), 0);
  }

  @Test
  public void testCompleteResponseRecordsWhenRecordRequestSucceeded() throws Exception {
    OperationContext sessionContext = session(UsageOperation.METADATA_WRITE.key());
    enricher.onSessionReady(sessionContext);
    enricher.completeResponse(100L);
    Assert.assertEquals(recordResponseCount.get(), 1);
  }

  @Test
  public void testNestedSessionsCompleteAllSessionsLifo() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            recordRequestCount.incrementAndGet();
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            recordResponseCount.incrementAndGet();
            responseBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher nestedEnricher =
        new UsageMetricsSessionEnricher(trackingStore, true);

    nestedEnricher.onSessionReady(session(UsageOperation.METADATA_READ.key()));
    nestedEnricher.onSessionReady(session(UsageOperation.METADATA_WRITE.key()));
    nestedEnricher.completeResponse(100L);

    Assert.assertEquals(recordResponseCount.get(), 2);
    Assert.assertEquals(responseBytes, Arrays.asList(100L, null));
  }

  @Test
  public void testGraphqlAsyncPath_outputBytesRecordedOnce() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            responseBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher graphqlEnricher =
        new UsageMetricsSessionEnricher(trackingStore, true);

    OperationContext sessionContext = graphqlSession();
    graphqlEnricher.onSessionReady(sessionContext);
    graphqlEnricher.completeResponse(null);
    graphqlEnricher.recordResponseWithBytes(sessionContext, 256L);

    Assert.assertEquals(responseBytes, Arrays.asList(null, 256L));
    long positiveByteCalls =
        responseBytes.stream().filter(bytes -> bytes != null && bytes > 0).count();
    Assert.assertEquals(positiveByteCalls, 1);
  }

  @Test
  public void testGraphqlAsyncPath_crossThread_outputBytesRecordedOnce() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            synchronized (responseBytes) {
              responseBytes.add(outputBytes);
            }
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher graphqlEnricher =
        new UsageMetricsSessionEnricher(trackingStore, true);

    OperationContext sessionContext = graphqlSession();
    graphqlEnricher.onSessionReady(sessionContext);

    CountDownLatch workerStarted = new CountDownLatch(1);
    Thread worker =
        new Thread(
            () -> {
              workerStarted.countDown();
              graphqlEnricher.recordResponseWithBytes(sessionContext, 256L);
            });
    worker.start();
    workerStarted.await();
    worker.join();

    graphqlEnricher.completeResponse(256L);

    long positiveByteCalls =
        responseBytes.stream().filter(bytes -> bytes != null && bytes > 0).count();
    Assert.assertEquals(positiveByteCalls, 1);
    Assert.assertEquals(
        responseBytes.stream().filter(bytes -> bytes != null && bytes > 0).findFirst().get(),
        Long.valueOf(256L));
  }

  @Test
  public void testSyncPath_unaffectedWhenOtherSessionMarkedAsync() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            responseBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher enricherUnderTest =
        new UsageMetricsSessionEnricher(trackingStore, true);

    OperationContext graphqlContext = graphqlSession();
    enricherUnderTest.recordResponseWithBytes(graphqlContext, 128L);

    Authentication syncAuthentication =
        new Authentication(new Actor(ActorType.USER, "syncuser"), "Basic test");
    RequestContext syncRequestContext =
        openapiBuilder()
            .usageOperation(UsageOperation.METADATA_READ.key())
            .usageIdentity("urn:li:corpuser:syncuser")
            .authChannel(AuthChannel.SESSION)
            .build();
    OperationContext syncContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .requestContext(syncRequestContext)
            .build(syncAuthentication, true);
    enricherUnderTest.onSessionReady(syncContext);
    enricherUnderTest.completeResponse(512L);

    Assert.assertEquals(responseBytes, Arrays.asList(128L, 512L));
  }

  @Test
  public void testFilterSyncPath_outputBytesFromCompleteResponse() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            responseBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher syncEnricher = new UsageMetricsSessionEnricher(trackingStore, true);

    OperationContext sessionContext = session(UsageOperation.METADATA_READ.key());
    syncEnricher.onSessionReady(sessionContext);
    syncEnricher.completeResponse(512L);

    Assert.assertEquals(responseBytes, List.of(512L));
  }

  @Test
  public void testDoublePathRisk_bothPathsWithBytes() throws Exception {
    List<Long> responseBytes = new ArrayList<>();
    UsageAggregationStore trackingStore =
        new UsageAggregationStore() {
          @Override
          public boolean recordRequest(@Nonnull OperationContext opContext) {
            return true;
          }

          @Override
          public void recordResponse(
              @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
            responseBytes.add(outputBytes);
          }

          @Override
          public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
        };
    UsageMetricsSessionEnricher riskEnricher = new UsageMetricsSessionEnricher(trackingStore, true);

    OperationContext sessionContext = graphqlSession();
    riskEnricher.onSessionReady(sessionContext);
    riskEnricher.recordResponseWithBytes(sessionContext, 128L);
    riskEnricher.completeResponse(128L);

    Assert.assertEquals(responseBytes, Arrays.asList(128L, null));
    long positiveByteCalls =
        responseBytes.stream().filter(bytes -> bytes != null && bytes > 0).count();
    Assert.assertEquals(positiveByteCalls, 1);
  }

  private static OperationContext session(String usageOperation) throws Exception {
    RequestContext.RequestContextBuilder builder = openapiBuilder();
    if (usageOperation != null) {
      builder.usageOperation(usageOperation);
    }
    RequestContext requestContext =
        builder
            .usageIdentity(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .authChannel(AuthChannel.SESSION)
            .build();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");
    return TestOperationContexts.systemContextNoValidate().toBuilder()
        .requestContext(requestContext)
        .build(authentication, true);
  }

  private static OperationContext graphqlSession() throws Exception {
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.GRAPHQL)
            .requestID("smokeUsageAggregationMe")
            .userAgent("")
            .usageOperation(UsageOperation.METADATA_QUERY.key())
            .usageIdentity(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .authChannel(AuthChannel.SESSION)
            .build();
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");
    return TestOperationContexts.systemContextNoValidate().toBuilder()
        .requestContext(requestContext)
        .build(authentication, true);
  }

  @Test
  public void testRecordTaggedServletRequestRecordsWithoutAsSession() throws Exception {
    RequestContext.RequestContextBuilder builder =
        openapiBuilder().usageOperation(UsageOperation.OTHER_READ.key());
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");

    enricher.recordTaggedServletRequest(
        TestOperationContexts.systemContextNoValidate(), builder, authentication);

    Assert.assertEquals(recordRequestCount.get(), 1);
  }

  @Test
  public void testDisabledEnricherSkipsAllPaths() throws Exception {
    UsageMetricsSessionEnricher disabled =
        new UsageMetricsSessionEnricher(
            new UsageAggregationStore() {
              @Override
              public boolean recordRequest(@Nonnull OperationContext opContext) {
                recordRequestCount.incrementAndGet();
                return true;
              }

              @Override
              public void recordResponse(
                  @Nonnull OperationContext opContext, @Nullable Long outputBytes) {
                recordResponseCount.incrementAndGet();
              }

              @Override
              public void flush(@Nonnull com.linkedin.metadata.usage.flush.FlushTrigger trigger) {}
            },
            false);
    RequestContext.RequestContextBuilder builder =
        openapiBuilder().usageOperation(UsageOperation.METADATA_READ.key());
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");

    disabled.enrichBeforeBuild(builder, authentication);
    disabled.onSessionReady(session(UsageOperation.METADATA_READ.key()));
    disabled.completeResponse(10L);
    disabled.recordResponseWithBytes(session(UsageOperation.METADATA_READ.key()), 10L);

    Assert.assertEquals(recordRequestCount.get(), 0);
    Assert.assertEquals(recordResponseCount.get(), 0);
  }

  @Test
  public void testEnrichBeforeBuildSkipsWhenRequestApiUnset() {
    RequestContext.RequestContextBuilder builder =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .usageOperation(UsageOperation.METADATA_READ.key());
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "metadatatest"), "Basic test");

    enricher.enrichBeforeBuild(builder, authentication);

    Assert.assertNull(builder.peekRequestAPI());
    Assert.assertNull(builder.peekGraphqlOperationKind());
  }

  private static RequestContext.RequestContextBuilder openapiBuilder() {
    return RequestContext.builder()
        .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
        .sourceIP("127.0.0.1")
        .requestAPI(RequestContext.RequestAPI.OPENAPI)
        .requestID("createEntity")
        .userAgent("");
  }
}
