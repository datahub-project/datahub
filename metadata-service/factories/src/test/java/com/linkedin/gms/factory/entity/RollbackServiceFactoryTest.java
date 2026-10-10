package com.linkedin.gms.factory.entity;

import static io.datahubproject.test.search.SearchTestUtils.TEST_SYSTEM_METADATA_SERVICE_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.service.IngestionRollbackDispatcher;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RollbackServiceFactoryTest {

  private static final String RUN_ID = "factory-test-run-id";

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityService<?> entityService;
  private SystemMetadataService systemMetadataService;
  private TimeseriesAspectService timeseriesAspectService;
  private ConfigurationProvider configurationProvider;

  @BeforeMethod
  public void setup() {
    entityService = mock(EntityService.class);
    systemMetadataService = mock(SystemMetadataService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getSystemMetadataService())
        .thenReturn(TEST_SYSTEM_METADATA_SERVICE_CONFIG);

    // One non-key row, so the in-process path needs no per-entity lookups.
    final AspectRowSummary row = new AspectRowSummary();
    row.setUrn("urn:li:dataset:(urn:li:dataPlatform:hive,factory-test-dataset,PROD)");
    row.setAspectName("datasetProperties");
    row.setRunId(RUN_ID);
    row.setKeyAspect(false);
    final List<AspectRowSummary> rows = new ArrayList<>(List.of(row));
    when(systemMetadataService.findByRunId(
            any(OperationContext.class), eq(RUN_ID), eq(false), anyInt(), anyInt()))
        .thenReturn(rows);
    when(entityService.rollbackRun(any(), anyList(), eq(RUN_ID), eq(false)))
        .thenReturn(new RollbackRunResult(rows, 0, Collections.emptyList()));
    final DeleteAspectValuesResult noTimeseriesDocs = new DeleteAspectValuesResult();
    noTimeseriesDocs.setNumDocsDeleted(0L);
    when(timeseriesAspectService.rollbackTimeseriesAspects(any(), eq(RUN_ID)))
        .thenReturn(noTimeseriesDocs);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void rollbackIsHandedToTheDispatcherBeanWhenOneIsAvailable() throws Exception {
    final IngestionRollbackDispatcher dispatcher = mock(IngestionRollbackDispatcher.class);
    when(dispatcher.dispatch(any(), anyString(), anyBoolean())).thenReturn(true);
    final ObjectProvider<IngestionRollbackDispatcher> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(dispatcher);

    final RollbackService service = newRollbackService(provider);
    service.rollbackIngestion(opContext, RUN_ID, false, false, null);

    verify(dispatcher, times(1)).dispatch(opContext, RUN_ID, false);
    verify(entityService, never()).rollbackRun(any(), any(), any(), anyBoolean());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void rollbackRunsInProcessWhenNoDispatcherBeanIsAvailable() throws Exception {
    final ObjectProvider<IngestionRollbackDispatcher> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(null);

    final RollbackService service = newRollbackService(provider);
    service.rollbackIngestion(opContext, RUN_ID, false, false, null);

    verify(entityService, times(1)).rollbackRun(any(), anyList(), eq(RUN_ID), eq(false));
  }

  private RollbackService newRollbackService(
      final ObjectProvider<IngestionRollbackDispatcher> provider) {
    return new RollbackServiceFactory()
        .rollbackService(
            entityService,
            systemMetadataService,
            timeseriesAspectService,
            configurationProvider,
            provider);
  }
}
