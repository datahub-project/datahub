package com.linkedin.metadata.test;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.test.query.QueryEngine;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testcontainers.shaded.com.google.common.cache.Cache;
import org.testcontainers.shaded.com.google.common.cache.CacheBuilder;
import org.testcontainers.shaded.com.google.common.cache.RemovalCause;
import org.testcontainers.shaded.com.google.common.cache.RemovalListener;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;


public class TestEngineTest {
  QueryEngine _queryEngine = mock(QueryEngine.class);
//  TestEngine _testEngine = new TestEngine(_queryEngine, new TestRuleEvaluator(UnitTestRuleEvaluator.getInstance()));

  DatasetUrn DATASET_URN = new DatasetUrn(new DataPlatformUrn("bigquery"), "test_dataset", FabricType.DEV);

  @BeforeTest
  public void setup() {
    reset(_queryEngine);
  }

  @Test
  public void testEngine() throws InterruptedException {
    Cache<String, String> testCache = CacheBuilder.newBuilder()
        .expireAfterWrite(2, TimeUnit.SECONDS)
        .removalListener((RemovalListener<String, String>) removalNotification -> {
          if (removalNotification.getCause() == RemovalCause.EXPIRED) {
            System.out.println("EXPIRED!! " + removalNotification.getKey());
          }
        })
        .build();
    ScheduledExecutorService cleanUpService = Executors.newScheduledThreadPool(1);
    cleanUpService.scheduleAtFixedRate(testCache::cleanUp, 0, 1,
        TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      System.out.println("pushing key1 - number: " + i);
      testCache.put("key1", "key1");
      if (i < 5) {
        System.out.println("pushing key2 - number: " + i);
        testCache.put("key2", "key2");
      }
      System.out.println("Sleeping");
      Thread.sleep(1000);
    }
    for (int i = 0; i < 10; i++) {
      System.out.println("Try " + i);
      Thread.sleep(1000);
    }
  }
}
