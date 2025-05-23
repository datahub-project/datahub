package io.openlineage.spark.agent.vendor.bigquery.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import scala.collection.immutable.Seq;

public class BigQueryRelationVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  BigQueryRelationVisitor<OpenLineage.OutputDataset> visitor;

  @Before
  public void setUp() {
    visitor = new BigQueryRelationVisitor<>(context, factory);
  }

  @Test
  public void testIsDefinedAt() {
    // Create a mock LogicalPlan that is not a LogicalRelation
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    assertFalse(visitor.isDefinedAt(mockPlan));

    // Create a mock LogicalRelation with a non-BigQuery relation
    LogicalRelation mockRelation = mock(LogicalRelation.class);
    BaseRelation mockBaseRelation = mock(BaseRelation.class);
    when(mockRelation.relation()).thenReturn(mockBaseRelation);
    when(mockBaseRelation.getClass().getName())
        .thenReturn("org.apache.spark.sql.sources.BaseRelation");
    assertFalse(visitor.isDefinedAt(mockRelation));

    // Create a mock LogicalRelation with a BigQuery relation
    LogicalRelation mockBqRelation = mock(LogicalRelation.class);
    BaseRelation mockBqBaseRelation = mock(BaseRelation.class);
    when(mockBqRelation.relation()).thenReturn(mockBqBaseRelation);
    when(mockBqBaseRelation.getClass().getName())
        .thenReturn("com.google.cloud.spark.bigquery.BigQueryRelation");
    assertTrue(visitor.isDefinedAt(mockBqRelation));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock LogicalRelation with a BigQuery relation
    LogicalRelation mockRelation = mock(LogicalRelation.class);
    BaseRelation mockBaseRelation = mock(BaseRelation.class);
    when(mockRelation.relation()).thenReturn(mockBaseRelation);
    when(mockBaseRelation.getClass().getName())
        .thenReturn("com.google.cloud.spark.bigquery.BigQueryRelation");

    // Mock schema
    StructType mockSchema = mock(StructType.class);
    when(mockRelation.schema()).thenReturn(mockSchema);

    // Mock output
    AttributeReference mockAttr = mock(AttributeReference.class);
    Seq<AttributeReference> mockOutput = mock(Seq.class);
    when(mockRelation.output()).thenReturn(mockOutput);
    when(mockOutput.isEmpty()).thenReturn(false);

    // Create test visitor with mocked method handling
    visitor =
        new BigQueryRelationVisitor<OpenLineage.OutputDataset>(context, factory) {
          protected String extractProperty(Object relation, String propertyName) {
            if ("project".equals(propertyName)) {
              return "test-project";
            } else if ("dataset".equals(propertyName)) {
              return "test_dataset";
            } else if ("table".equals(propertyName)) {
              return "test_table";
            }
            return null;
          }
        };

    // Mock dataset factory
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    when(factory.getDataset("test-project.test_dataset.test_table", "bigquery", mockSchema))
        .thenReturn(mockDataset);

    // Test apply method
    List<OpenLineage.OutputDataset> result = visitor.apply(mockRelation);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
