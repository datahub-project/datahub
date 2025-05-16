package io.openlineage.spark.agent.vendor.synapse.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import scala.collection.immutable.Seq;

public class SynapseRelationVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  SynapseRelationVisitor<OpenLineage.OutputDataset> visitor;

  @Before
  public void setUp() {
    visitor = new SynapseRelationVisitor<>(context, factory);
  }

  @Test
  public void testIsDefinedAt() {
    // Create a mock LogicalPlan that is not a LogicalRelation
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    assertFalse(visitor.isDefinedAt(mockPlan));

    // Create a mock LogicalRelation with a non-Synapse relation
    LogicalRelation mockRelation = mock(LogicalRelation.class);
    BaseRelation mockBaseRelation = mock(BaseRelation.class);
    when(mockRelation.relation()).thenReturn(mockBaseRelation);
    when(mockBaseRelation.getClass().getName())
        .thenReturn("org.apache.spark.sql.sources.BaseRelation");
    assertFalse(visitor.isDefinedAt(mockRelation));

    // Create a mock LogicalRelation with a Synapse relation
    LogicalRelation mockSynapseRelation = mock(LogicalRelation.class);
    BaseRelation mockSynapseBaseRelation = mock(BaseRelation.class);
    when(mockSynapseRelation.relation()).thenReturn(mockSynapseBaseRelation);
    when(mockSynapseBaseRelation.getClass().getName())
        .thenReturn("com.microsoft.spark.synapse.connector.SynapseRelation");
    assertTrue(visitor.isDefinedAt(mockSynapseRelation));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock LogicalRelation with a Synapse relation
    LogicalRelation mockRelation = mock(LogicalRelation.class);
    BaseRelation mockBaseRelation = mock(BaseRelation.class);
    when(mockRelation.relation()).thenReturn(mockBaseRelation);
    when(mockBaseRelation.getClass().getName())
        .thenReturn("com.microsoft.spark.synapse.connector.SynapseRelation");

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
        new SynapseRelationVisitor<OpenLineage.OutputDataset>(context, factory) {
          protected Map<String, String> extractParameters(Object synapseRelation) {
            Map<String, String> params = new HashMap<>();
            params.put("serverName", "test-server");
            params.put("databaseName", "test_db");
            params.put("tableName", "test_table");
            return params;
          }
        };

    // Mock dataset factory
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    when(factory.getDataset("test-server.test_db.test_table", "synapse", mockSchema))
        .thenReturn(mockDataset);

    // Test apply method
    List<OpenLineage.OutputDataset> result = visitor.apply(mockRelation);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
