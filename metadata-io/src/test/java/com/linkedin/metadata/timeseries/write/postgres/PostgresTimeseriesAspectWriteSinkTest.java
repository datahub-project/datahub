package com.linkedin.metadata.timeseries.write.postgres;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PgTimeseriesStoreOptions;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry;
import com.linkedin.metadata.timeseries.postgres.PgTimeseriesStoreRegistry.StoreHandle;
import com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PostgresTimeseriesAspectWriteSinkTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void upsert_sqlException_incrementsMetricAndSwallowsWhenFailOnErrorFalse()
      throws Exception {
    PostgresTimeseriesAspectDao dao = mock(PostgresTimeseriesAspectDao.class);
    doThrow(new SQLException("boom")).when(dao).upsert(any());
    MetricUtils metricUtils = mock(MetricUtils.class);
    OperationContext opContext =
        Mockito.spy(TestOperationContexts.systemContextNoSearchAuthorization());
    Mockito.doReturn(Optional.of(metricUtils)).when(opContext).getMetricUtils();

    PostgresTimeseriesAspectWriteSink sink =
        new PostgresTimeseriesAspectWriteSink(registryWithDao(dao), false);
    sink.upsertDocument(opContext, "dataset", "datasetProfile", "doc1", sampleDoc());

    verify(metricUtils)
        .increment(
            eq(PostgresTimeseriesAspectWriteSink.class),
            eq(PostgresTimeseriesAspectWriteSink.UPSERT_FAILURE_METRIC),
            eq(1.0));
  }

  @Test
  public void upsert_sqlException_throwsWhenFailOnErrorTrue() throws Exception {
    PostgresTimeseriesAspectDao dao = mock(PostgresTimeseriesAspectDao.class);
    doThrow(new SQLException("boom")).when(dao).upsert(any());
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();

    PostgresTimeseriesAspectWriteSink sink =
        new PostgresTimeseriesAspectWriteSink(registryWithDao(dao), true);
    assertThrows(
        IllegalStateException.class,
        () -> sink.upsertDocument(opContext, "dataset", "datasetProfile", "doc1", sampleDoc()));
  }

  private static PgTimeseriesStoreRegistry registryWithDao(PostgresTimeseriesAspectDao dao) {
    PgTimeseriesStoreOptions store =
        PgTimeseriesStoreOptions.builder()
            .name("default")
            .schema("public")
            .tablePrefix("metadata_timeseries")
            .partmanPartitionInterval("1 day")
            .partmanPremake(4)
            .retentionMaxAgeSeconds(7776000)
            .maintenanceIntervalSeconds(3600)
            .poolMinConnections(1)
            .poolMaxConnections(12)
            .poolMaxInactiveTimeSeconds(120)
            .poolMaxAgeMinutes(120)
            .poolLeakTimeMinutes(15)
            .poolWaitTimeoutMillis(1000)
            .build();
    StoreHandle handle = new StoreHandle(store, mock(Database.class), dao);
    return new PgTimeseriesStoreRegistry(
        new PgTimeseriesSetupOptions("default", Map.of("default", store), Map.of()),
        Map.of("default", handle));
  }

  private static ObjectNode sampleDoc() {
    ObjectNode doc = MAPPER.createObjectNode();
    doc.put(MappingsBuilder.URN_FIELD, "urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
    doc.put(MappingsBuilder.TIMESTAMP_MILLIS_FIELD, 1_700_000_000_000L);
    return doc;
  }
}
