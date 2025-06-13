package io.openlineage.spark.agent.vendor.delta.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import java.util.List;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class DeltaTableHelperTest {

  @Test
  void testGetDatasetWithNullTableName() {
    DatasetFactory<OpenLineage.Dataset> factory = mock(DatasetFactory.class);
    List<OpenLineage.Dataset> result = DeltaTableHelper.getDataset(factory, null, null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetDatasetWithEmptyTableName() {
    DatasetFactory<OpenLineage.Dataset> factory = mock(DatasetFactory.class);
    List<OpenLineage.Dataset> result = DeltaTableHelper.getDataset(factory, "", null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testGetDatasetWithoutSchema() {
    DatasetFactory<OpenLineage.Dataset> factory = mock(DatasetFactory.class);
    OpenLineage.Dataset mockDataset = mock(OpenLineage.Dataset.class);
    when(factory.getDataset(anyString(), anyString())).thenReturn(mockDataset);

    List<OpenLineage.Dataset> result = DeltaTableHelper.getDataset(factory, "test_table", null);

    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }

  @Test
  void testGetDatasetWithSchema() {
    DatasetFactory<OpenLineage.Dataset> factory = mock(DatasetFactory.class);
    OpenLineage.Dataset mockDataset = mock(OpenLineage.Dataset.class);
    StructType schema = new StructType();

    when(factory.getDataset(anyString(), anyString(), isNull())).thenReturn(mockDataset);

    List<OpenLineage.Dataset> result = DeltaTableHelper.getDataset(factory, "test_table", schema);

    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
