package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchHitMatches;
import java.util.List;
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
}
