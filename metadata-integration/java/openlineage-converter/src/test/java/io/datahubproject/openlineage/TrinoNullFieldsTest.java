package io.datahubproject.openlineage;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import com.linkedin.common.FabricType;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.datahubproject.openlineage.dataset.DatahubJob;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.testng.annotations.Test;

/**
 * Tests to reproduce NPE issues when Trino sends OpenLineage COMPLETE events with
 * SchemaDatasetFacet or ColumnLineageDatasetFacet containing null fields.
 *
 * <p>These tests reproduce the issue reported in support ticket #6284: Trino OpenLineage COMPLETE
 * events cause HTTP 500 errors in DataHub integration
 *
 * <p>Root cause: OpenLineageToDataHub.java doesn't check if getFields() returns null before calling
 * methods on it.
 *
 * <p>OpenLineage Spec References:
 *
 * <ul>
 *   <li>SchemaDatasetFacet: https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json -
 *       "fields" is NOT required, can be omitted
 *   <li>ColumnLineageDatasetFacet:
 *       https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json - "fields" IS
 *       required but can be an empty object {} - OpenLineage Java client may return null when
 *       fields is empty or omitted
 * </ul>
 */
public class TrinoNullFieldsTest {

  private static final URI TRINO_PRODUCER_URI = URI.create("https://github.com/trinodb/trino/");

  /**
   * Test that reproduces Bug #1: NPE in getFineGrainedLineage() when
   * ColumnLineageDatasetFacet.getFields() returns null.
   *
   * <p>Location: OpenLineageToDataHub.java line 424 Code:
   * columnLineage.getFields().getAdditionalProperties().entrySet()
   */
  @Test
  public void testColumnLineageFacetWithNullFields() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("trino")
            .materializeDataset(true)
            .captureColumnLevelLineage(true) // This enables the problematic code path
            .build();

    OpenLineage openLineage = new OpenLineage(TRINO_PRODUCER_URI);

    // Create a ColumnLineageDatasetFacet with null fields (simulating Trino behavior)
    // In OpenLineage spec, this is valid - fields can be null or empty
    OpenLineage.ColumnLineageDatasetFacet columnLineageFacet =
        openLineage
            .newColumnLineageDatasetFacetBuilder()
            // Note: Not setting fields - this makes getFields() return null
            .build();

    // Create dataset facets with the column lineage facet
    OpenLineage.DatasetFacets datasetFacets =
        openLineage.newDatasetFacetsBuilder().columnLineage(columnLineageFacet).build();

    // Create an output dataset with the facets
    OpenLineage.OutputDataset outputDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("trino://my-cluster")
            .name("catalog.schema.table")
            .facets(datasetFacets)
            .build();

    // Create a COMPLETE event (this is when the NPE occurs)
    OpenLineage.RunEvent runEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(openLineage.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("trino://my-cluster")
                    .name("test_query")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Arrays.asList(outputDataset))
            .build();

    try {
      DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, config);
      assertNotNull(
          datahubJob, "Should handle null fields in ColumnLineageDatasetFacet gracefully");
    } catch (NullPointerException e) {
      fail(
          "NPE thrown when ColumnLineageDatasetFacet.getFields() is null - this is Bug #1!\n"
              + "Stack trace: "
              + e.getMessage());
    }
  }

  /**
   * Test that reproduces Bug #2: NPE in getSchemaMetadata() when SchemaDatasetFacet.getFields()
   * returns null.
   *
   * <p>Location: OpenLineageToDataHub.java line 1315 Code:
   * dataset.getFacets().getSchema().getFields().forEach(...) And line 1333:
   * OpenLineageClientUtils.toJson(dataset.getFacets().getSchema().getFields())
   */
  @Test
  public void testSchemaFacetWithNullFields() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("trino")
            .materializeDataset(true) // This enables the problematic code path
            .captureColumnLevelLineage(false)
            .build();

    OpenLineage openLineage = new OpenLineage(TRINO_PRODUCER_URI);

    // Create a SchemaDatasetFacet with null fields (simulating Trino behavior)
    // In OpenLineage spec, this is valid - fields can be null or empty
    OpenLineage.SchemaDatasetFacet schemaFacet =
        openLineage
            .newSchemaDatasetFacetBuilder()
            // Note: Not setting fields - this makes getFields() return null
            .build();

    // Create dataset facets with the schema facet
    OpenLineage.DatasetFacets datasetFacets =
        openLineage.newDatasetFacetsBuilder().schema(schemaFacet).build();

    // Create an output dataset with the facets
    OpenLineage.OutputDataset outputDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("trino://my-cluster")
            .name("catalog.schema.table")
            .facets(datasetFacets)
            .build();

    // Create a COMPLETE event (this is when the NPE occurs)
    OpenLineage.RunEvent runEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(openLineage.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("trino://my-cluster")
                    .name("test_query")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Arrays.asList(outputDataset))
            .build();

    try {
      DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, config);
      assertNotNull(datahubJob, "Should handle null fields in SchemaDatasetFacet gracefully");
    } catch (NullPointerException e) {
      fail(
          "NPE thrown when SchemaDatasetFacet.getFields() is null - this is Bug #2!\n"
              + "Stack trace: "
              + e.getMessage());
    }
  }

  /**
   * Test that reproduces both bugs together - simulating a real Trino COMPLETE event that includes
   * both SchemaDatasetFacet and ColumnLineageDatasetFacet with null fields.
   */
  @Test
  public void testTrinoCompleteEventWithNullFieldsInBothFacets() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("trino")
            .materializeDataset(true)
            .captureColumnLevelLineage(true)
            .build();

    OpenLineage openLineage = new OpenLineage(TRINO_PRODUCER_URI);

    // Create facets with null fields (both bugs)
    OpenLineage.SchemaDatasetFacet schemaFacet = openLineage.newSchemaDatasetFacetBuilder().build();

    OpenLineage.ColumnLineageDatasetFacet columnLineageFacet =
        openLineage.newColumnLineageDatasetFacetBuilder().build();

    OpenLineage.DatasetFacets datasetFacets =
        openLineage
            .newDatasetFacetsBuilder()
            .schema(schemaFacet)
            .columnLineage(columnLineageFacet)
            .build();

    OpenLineage.OutputDataset outputDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("trino://my-cluster")
            .name("catalog.schema.table")
            .facets(datasetFacets)
            .build();

    OpenLineage.RunEvent runEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(openLineage.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("trino://my-cluster")
                    .name("INSERT INTO catalog.schema.table SELECT * FROM source")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Arrays.asList(outputDataset))
            .build();

    try {
      DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, config);
      assertNotNull(datahubJob, "Should handle Trino COMPLETE events with null fields gracefully");
    } catch (NullPointerException e) {
      fail(
          "NPE thrown when processing Trino COMPLETE event - this is the reported bug!\n"
              + "Exception: "
              + e.getClass().getName()
              + ": "
              + e.getMessage());
    }
  }

  /**
   * Test handling of ColumnLineageDatasetFacet with empty fields object.
   *
   * <p>Per OpenLineage spec
   * (https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json), "fields" is required
   * but can be an empty object {}. The OpenLineage Java client may return a non-null fields object
   * with empty additionalProperties in this case.
   */
  @Test
  public void testColumnLineageFacetWithEmptyFields() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("trino")
            .materializeDataset(true)
            .captureColumnLevelLineage(true)
            .build();

    OpenLineage openLineage = new OpenLineage(TRINO_PRODUCER_URI);

    // Create a ColumnLineageDatasetFacet with explicitly empty fields
    // Per spec, fields can be an empty object {} when there are no column-level lineages
    OpenLineage.ColumnLineageDatasetFacetFields emptyFields =
        openLineage.newColumnLineageDatasetFacetFieldsBuilder().build();

    OpenLineage.ColumnLineageDatasetFacet columnLineageFacet =
        openLineage.newColumnLineageDatasetFacetBuilder().fields(emptyFields).build();

    OpenLineage.DatasetFacets datasetFacets =
        openLineage.newDatasetFacetsBuilder().columnLineage(columnLineageFacet).build();

    OpenLineage.OutputDataset outputDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("trino://my-cluster")
            .name("catalog.schema.table")
            .facets(datasetFacets)
            .build();

    OpenLineage.RunEvent runEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(openLineage.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("trino://my-cluster")
                    .name("test_query_empty_lineage")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Arrays.asList(outputDataset))
            .build();

    try {
      DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, config);
      assertNotNull(
          datahubJob, "Should handle empty fields object in ColumnLineageDatasetFacet gracefully");
    } catch (NullPointerException e) {
      fail(
          "NPE thrown when ColumnLineageDatasetFacet has empty fields object!\n"
              + "Stack trace: "
              + e.getMessage());
    }
  }

  /**
   * Test handling of SchemaDatasetFacet with empty fields list.
   *
   * <p>Per OpenLineage spec (https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json),
   * "fields" is NOT required and can be omitted or set to an empty array [].
   */
  @Test
  public void testSchemaFacetWithEmptyFieldsList() throws Exception {
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("trino")
            .materializeDataset(true)
            .captureColumnLevelLineage(false)
            .build();

    OpenLineage openLineage = new OpenLineage(TRINO_PRODUCER_URI);

    // Create a SchemaDatasetFacet with explicitly empty fields list
    OpenLineage.SchemaDatasetFacet schemaFacet =
        openLineage.newSchemaDatasetFacetBuilder().fields(Collections.emptyList()).build();

    OpenLineage.DatasetFacets datasetFacets =
        openLineage.newDatasetFacetsBuilder().schema(schemaFacet).build();

    OpenLineage.OutputDataset outputDataset =
        openLineage
            .newOutputDatasetBuilder()
            .namespace("trino://my-cluster")
            .name("catalog.schema.table")
            .facets(datasetFacets)
            .build();

    OpenLineage.RunEvent runEvent =
        openLineage
            .newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(openLineage.newRunBuilder().runId(UUID.randomUUID()).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .namespace("trino://my-cluster")
                    .name("test_query_empty_schema")
                    .facets(openLineage.newJobFacetsBuilder().build())
                    .build())
            .inputs(Collections.emptyList())
            .outputs(Arrays.asList(outputDataset))
            .build();

    try {
      DatahubJob datahubJob = OpenLineageToDataHub.convertRunEventToJob(runEvent, config);
      assertNotNull(datahubJob, "Should handle empty fields list in SchemaDatasetFacet gracefully");
    } catch (NullPointerException e) {
      fail(
          "NPE thrown when SchemaDatasetFacet has empty fields list!\n"
              + "Stack trace: "
              + e.getMessage());
    }
  }
}
