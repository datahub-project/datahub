package com.linkedin.metadata.timeseries;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Direct unit tests for the atomic step that runs server-side on Hazelcast. The Caffeine path has
 * its own equivalent compare-and-set inside the caching service; this class covers what runs on a
 * Hazelcast partition owner when we call {@code IMap.executeOnKey(...)}.
 */
public class PutIfNewerProcessorTest {

  private static final String KEY = "latest:dataset:datasetProfile:urn:li:dataset:foo";
  private static final String SERIALIZED = "{\"aspect\":\"value\"}";

  @Test
  public void process_emptySlot_writesIncoming() {
    CachedLatestAspect incoming = new CachedLatestAspect(1000L, SERIALIZED);
    Map.Entry<String, CachedLatestAspect> entry = new AbstractMap.SimpleEntry<>(KEY, null);

    Boolean accepted = new PutIfNewerProcessor(incoming).process(entry);

    assertTrue(accepted, "empty slot must accept the incoming wrapper");
    assertSame(entry.getValue(), incoming);
  }

  @Test
  public void process_olderExisting_overwrites() {
    CachedLatestAspect existing = new CachedLatestAspect(500L, "old");
    CachedLatestAspect incoming = new CachedLatestAspect(1000L, SERIALIZED);
    Map.Entry<String, CachedLatestAspect> entry = new AbstractMap.SimpleEntry<>(KEY, existing);

    Boolean accepted = new PutIfNewerProcessor(incoming).process(entry);

    assertTrue(accepted);
    assertSame(entry.getValue(), incoming);
  }

  @Test
  public void process_equalTimestamp_overwrites() {
    // The comparison is >=, so a write with the same event time wins. This is intentional
    // because two events at the same timestampMillis are typically the same logical event,
    // and accepting the freshest write is more useful than rejecting it.
    CachedLatestAspect existing = new CachedLatestAspect(1000L, "first");
    CachedLatestAspect incoming = new CachedLatestAspect(1000L, "second");
    Map.Entry<String, CachedLatestAspect> entry = new AbstractMap.SimpleEntry<>(KEY, existing);

    Boolean accepted = new PutIfNewerProcessor(incoming).process(entry);

    assertTrue(accepted);
    assertSame(entry.getValue(), incoming);
  }

  @Test
  public void process_newerExisting_rejects() {
    CachedLatestAspect existing = new CachedLatestAspect(2000L, "newer");
    CachedLatestAspect incoming = new CachedLatestAspect(1000L, "older");
    Map.Entry<String, CachedLatestAspect> entry = new AbstractMap.SimpleEntry<>(KEY, existing);

    Boolean accepted = new PutIfNewerProcessor(incoming).process(entry);

    assertFalse(accepted, "older incoming must not clobber a newer cached entry");
    assertSame(entry.getValue(), existing, "existing wrapper must be preserved verbatim");
  }
}
