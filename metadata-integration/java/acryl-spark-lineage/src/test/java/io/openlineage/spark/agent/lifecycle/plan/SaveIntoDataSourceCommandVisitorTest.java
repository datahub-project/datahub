package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Covers the catalog-based Delta {@code saveAsTable} table-name recovery restored in PR #14911:
 * when a Delta write carries neither a "path" nor a "table" option, the visitor falls back to
 * parsing the CREATE TABLE statement from the query execution's SQL text. These tests pin the
 * parsing heuristic (quoting/db-qualifier stripping, IF NOT EXISTS handling, non-matching SQL).
 */
class SaveIntoDataSourceCommandVisitorTest {

  @Test
  void parsesPlainCreateTableName() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName("CREATE TABLE events USING delta"));
  }

  @Test
  void stripsDatabaseQualifierAndQuoting() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "CREATE TABLE `my_db`.`events` USING delta"));
  }

  @Test
  void skipsIfNotExistsQualifierAndReturnsRealName() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "create table if not exists events using delta"));
  }

  @Test
  void skipsIfNotExistsAndStripsDatabaseQualifier() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "CREATE TABLE IF NOT EXISTS my_db.events USING delta"));
  }

  @Test
  void returnsNullForNonCreateTableSql() {
    assertNull(
        SaveIntoDataSourceCommandVisitor.parseCreateTableName("INSERT INTO events VALUES (1)"));
  }

  @Test
  void returnsNullForNullSql() {
    assertNull(SaveIntoDataSourceCommandVisitor.parseCreateTableName(null));
  }

  /**
   * The visitor only checks that the data source class name contains "DeltaDataSource", so a stub
   * stands in for Delta, which is compile-only for this module.
   */
  abstract static class DeltaDataSource implements CreatableRelationProvider {}

  /**
   * DataHub's catalog-based Delta {@code saveAsTable} branch follows OpenLineage 1.54's save-mode
   * semantics: Append and Ignore leave an existing table in place, so they carry no lifecycle
   * facet.
   */
  @Test
  void catalogDeltaWriteDerivesLifecycleStateChangeFromSaveMode() {
    Map<SaveMode, LifecycleStateChange> expectedByMode = new EnumMap<>(SaveMode.class);
    expectedByMode.put(SaveMode.Overwrite, OVERWRITE);
    expectedByMode.put(SaveMode.ErrorIfExists, CREATE);
    expectedByMode.put(SaveMode.Append, null);
    expectedByMode.put(SaveMode.Ignore, null);

    expectedByMode.forEach(
        (mode, expected) -> {
          List<OpenLineage.OutputDataset> datasets = applyCatalogDeltaWrite(mode);

          assertEquals(1, datasets.size(), mode.name());
          assertEquals("my_table", datasets.get(0).getName(), mode.name());
          LifecycleStateChangeDatasetFacet facet =
              datasets.get(0).getFacets().getLifecycleStateChange();
          assertEquals(
              expected, facet == null ? null : facet.getLifecycleStateChange(), mode.name());
        });
  }

  private static List<OpenLineage.OutputDataset> applyCatalogDeltaWrite(SaveMode mode) {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
    when(context.getSparkSession()).thenReturn(Optional.of(mock(SparkSession.class)));
    when(context.getSparkExtensionVisitorWrapper())
        .thenReturn(mock(SparkOpenLineageExtensionVisitorWrapper.class));
    when(context.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());

    SaveIntoDataSourceCommand command = mock(SaveIntoDataSourceCommand.class);
    when(command.dataSource()).thenReturn(mock(DeltaDataSource.class));
    when(command.options())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(Collections.singletonMap("table", "my_table")));
    when(command.schema()).thenReturn(new StructType().add("id", DataTypes.IntegerType));
    when(command.mode()).thenReturn(mode);

    return new SaveIntoDataSourceCommandVisitor(context)
        .apply(mock(SparkListenerEvent.class), command);
  }
}
