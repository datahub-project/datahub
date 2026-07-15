package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.MCL_HEADER_DATABASE_ASPECT_VERSION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.mxe.MetadataChangeLog;
import org.testng.annotations.Test;

public class PostgresEntitySearchStrategyTest {

  @Test
  public void pgSearchIncludesDeletesRegardlessOfHeader() {
    MCLItem event = mock(MCLItem.class);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setHeaders(
        new StringMap(
            java.util.Map.of(
                MCL_HEADER_DATABASE_ASPECT_VERSION, Long.toString(ASPECT_LATEST_VERSION + 1))));
    when(event.getChangeType()).thenReturn(ChangeType.DELETE);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    assertTrue(PostgresEntitySearchStrategy.isDatabaseLatestAspectRowForPgSearch(event));
  }

  @Test
  public void pgSearchSkipsNonLatestRowWhenHeaderPresent() {
    MCLItem event = mock(MCLItem.class);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setHeaders(
        new StringMap(
            java.util.Map.of(
                MCL_HEADER_DATABASE_ASPECT_VERSION, Long.toString(ASPECT_LATEST_VERSION + 1))));
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    when(event.getUrn())
        .thenReturn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)"));
    when(event.getAspectName()).thenReturn("datasetProperties");
    assertFalse(PostgresEntitySearchStrategy.isDatabaseLatestAspectRowForPgSearch(event));
  }

  @Test
  public void pgSearchIncludesLatestRowHeader() {
    MCLItem event = mock(MCLItem.class);
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setHeaders(
        new StringMap(
            java.util.Map.of(
                MCL_HEADER_DATABASE_ASPECT_VERSION, Long.toString(ASPECT_LATEST_VERSION))));
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    assertTrue(PostgresEntitySearchStrategy.isDatabaseLatestAspectRowForPgSearch(event));
  }

  @Test
  public void pgSearchIncludesWhenHeaderAbsent() {
    MCLItem event = mock(MCLItem.class);
    MetadataChangeLog mcl = new MetadataChangeLog();
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    assertTrue(PostgresEntitySearchStrategy.isDatabaseLatestAspectRowForPgSearch(event));
  }
}
