package com.linkedin.datahub.upgrade.restorebackup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.ReaderWrapper;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.sql.Timestamp;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreStorageStepTest {

  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,restored,PROD)");
  // Owners alice and an owner of an entity type only a newer version registered.
  private static final String OWNERSHIP_FROM_NEWER_VERSION =
      "{\"owners\":[{\"owner\":\"urn:li:corpuser:alice\",\"type\":\"DATAOWNER\"},"
          + "{\"owner\":\"urn:li:entityFromNewerBuild:bot\",\"type\":\"DATAOWNER\"}]}";
  private static final String LEFT_OUT = "rows written by a newer version";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityService<?> entityService;
  private UpgradeContext context;
  private UpgradeReport report;

  @BeforeMethod
  public void setUp() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    entityService = mock(EntityService.class);
    when(entityService.ingestAspects(
            any(OperationContext.class), any(Urn.class), anyList(), any(AuditStamp.class), any()))
        .thenReturn(List.of(mock(UpdateAspectResult.class)));
    report = mock(UpgradeReport.class);
    context = mock(UpgradeContext.class);
    when(context.opContext()).thenReturn(opContext);
    when(context.report()).thenReturn(report);
  }

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testReportsARowRestoredWithoutTheNewerVersionsReferences() {
    // The table is cleared before the restore, so the references the read leaves out are lost:
    // the report names the row.
    new RestoreStorageStep(entityService, opContext.getEntityRegistry())
        .readerExecutable(readerOf(OWNERSHIP_FROM_NEWER_VERSION), context);

    // One summary line per backup file: rows per aspect, and examples.
    verify(report)
        .addLine(
            argThat(
                (String line) ->
                    line.startsWith("backup.parquet: " + LEFT_OUT)
                        && line.contains("{ownership=1}")
                        && line.contains("ownership of " + DATASET)));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Pair<String, RecordTemplate>>> written =
        ArgumentCaptor.forClass((Class) List.class);
    verify(entityService, timeout(5000))
        .ingestAspects(
            any(OperationContext.class),
            eq(DATASET),
            written.capture(),
            any(AuditStamp.class),
            any());
    assertEquals(new Ownership(written.getValue().get(0).getSecond().data()).getOwners().size(), 1);
  }

  @Test
  public void testReportsNothingForARowThisVersionKnows() {
    new RestoreStorageStep(entityService, opContext.getEntityRegistry())
        .readerExecutable(
            readerOf("{\"owners\":[{\"owner\":\"urn:li:corpuser:alice\",\"type\":\"DATAOWNER\"}]}"),
            context);

    verify(report, never()).addLine(argThat((String line) -> line.contains(LEFT_OUT)));
  }

  @Test
  public void testStrictHandlingReportsNothing() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);

    new RestoreStorageStep(entityService, opContext.getEntityRegistry())
        .readerExecutable(readerOf(OWNERSHIP_FROM_NEWER_VERSION), context);

    // Restored as before the tolerance: the row is written as read, with nothing reported.
    verify(report, never()).addLine(argThat((String line) -> line.contains(LEFT_OUT)));
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Pair<String, RecordTemplate>>> written =
        ArgumentCaptor.forClass((Class) List.class);
    verify(entityService, timeout(5000))
        .ingestAspects(
            any(OperationContext.class),
            eq(DATASET),
            written.capture(),
            any(AuditStamp.class),
            any());
    assertEquals(new Ownership(written.getValue().get(0).getSecond().data()).getOwners().size(), 2);
  }

  private static ReaderWrapper<?> readerOf(final String ownership) {
    ReaderWrapper<?> reader = mock(ReaderWrapper.class);
    when(reader.next())
        .thenReturn(
            new EbeanAspectV2(
                DATASET.toString(),
                "ownership",
                0,
                ownership,
                new Timestamp(0),
                "urn:li:corpuser:datahub",
                null,
                null))
        .thenReturn(null);
    when(reader.getFileName()).thenReturn("backup.parquet");
    return reader;
  }
}
