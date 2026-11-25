package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchHitMatches;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class HighlightCarryTest {

  @Test
  public void testOnlyTheFinalPageIsScanned() {
    AtomicInteger scans = new AtomicInteger();
    HighlightCarry.begin();
    try {
      HighlightCarry.stash(
          "urn:li:corpuser:kept",
          explain -> {
            scans.incrementAndGet();
            return SearchHitMatches.EMPTY;
          });
      HighlightCarry.stash(
          "urn:li:corpuser:discarded",
          explain -> {
            scans.incrementAndGet();
            return SearchHitMatches.EMPTY;
          });
      HighlightCarry.apply(
          List.of(new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:corpuser:kept"))), false);
      assertEquals(scans.get(), 1);
    } finally {
      HighlightCarry.clear();
    }
  }

  @Test
  public void testStageFillDoesNotScanTheHighlightReporter() {
    AtomicInteger scans = new AtomicInteger();
    HighlightCarry.begin();
    try {
      HighlightCarry.stash(
          "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
          explain -> {
            scans.incrementAndGet();
            return SearchHitMatches.EMPTY;
          });
      StageMatchedFields.stash(
          "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
          Map.of("name", "orders"),
          new MatchedFields("orders", 3),
          Map.of());
      StageMatchedFields.fill(
          List.of(
              new SearchEntity()
                  .setEntity(
                      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)"))),
          List.of("name"));
      assertEquals(scans.get(), 0);
    } finally {
      HighlightCarry.clear();
    }
  }

  @Test
  public void testBeginClearsAPriorStageStash() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)";
    HighlightCarry.begin();
    try {
      StageMatchedFields.stash(
          urn, Map.of("name", "orders"), new MatchedFields("orders", 3), Map.of());
      HighlightCarry.begin();
      SearchEntity hit = new SearchEntity().setEntity(UrnUtils.getUrn(urn));
      StageMatchedFields.fill(List.of(hit), List.of("name"));
      assertFalse(hit.hasMatchedFields());
    } finally {
      HighlightCarry.clear();
    }
  }
}
