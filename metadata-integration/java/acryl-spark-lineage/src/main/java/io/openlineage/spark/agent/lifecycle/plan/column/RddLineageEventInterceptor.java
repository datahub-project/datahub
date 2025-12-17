/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Intercepts OpenLineage events to track schemas and correct RDD lineage issues.
 *
 * <p>This class sits between the event builder and the emitter. It: 1. Tracks dataset schemas from
 * input/output events 2. Detects schema mismatches that indicate RDD conversions 3. Applies
 * positional mapping corrections to fix column lineage
 *
 * <p>Usage:
 *
 * <pre>
 * RddLineageEventInterceptor interceptor = new RddLineageEventInterceptor(config);
 * RunEvent correctedEvent = interceptor.intercept(event);
 * emitter.emit(correctedEvent);
 * </pre>
 */
@Slf4j
public class RddLineageEventInterceptor {

  private final SchemaHistoryTracker schemaTracker;
  private final ColumnLineageCorrector lineageCorrector;
  private final boolean enabled;

  /** Create a new interceptor with default settings. */
  public RddLineageEventInterceptor() {
    this(true);
  }

  /**
   * Create a new interceptor.
   *
   * @param enabled whether RDD lineage correction is enabled
   */
  public RddLineageEventInterceptor(boolean enabled) {
    this(enabled, new SchemaHistoryTracker());
  }

  /**
   * Create a new interceptor with custom schema tracker.
   *
   * @param enabled whether RDD lineage correction is enabled
   * @param schemaTracker schema history tracker
   */
  public RddLineageEventInterceptor(boolean enabled, SchemaHistoryTracker schemaTracker) {
    this.enabled = enabled;
    this.schemaTracker = schemaTracker;
    this.lineageCorrector = new ColumnLineageCorrector(schemaTracker);

    if (enabled) {
      log.info("RDD lineage correction is ENABLED");
    } else {
      log.info("RDD lineage correction is DISABLED");
    }
  }

  /**
   * Intercept a RunEvent to track schemas and apply corrections.
   *
   * @param event the original event
   * @return corrected event (may be the same instance if disabled or no corrections needed)
   */
  public RunEvent intercept(RunEvent event) {
    if (!enabled) {
      return event;
    }

    // Enhance INPUT datasets with missing schemas
    event = enhanceInputSchemas(event);

    // Track schemas
    trackSchemas(event);

    // Apply corrections
    return lineageCorrector.correctLineage(event);
  }

  /**
   * Enhance INPUT datasets that don't have schema facets by reading from file metadata.
   *
   * <p>This ensures that RDD operations have access to original file schemas, enabling proper
   * schema mismatch detection and column lineage correction.
   *
   * @param event the original event
   * @return event with enhanced INPUT datasets (may be the same instance if no changes needed)
   */
  private RunEvent enhanceInputSchemas(RunEvent event) {
    if (event.getInputs() == null || event.getInputs().isEmpty()) {
      return event;
    }

    List<InputDataset> enhancedInputs = new ArrayList<>();
    boolean anyEnhanced = false;

    for (InputDataset input : event.getInputs()) {
      // Check if this input already has a schema
      if (input.getFacets() != null && input.getFacets().getSchema() != null) {
        enhancedInputs.add(input);
        continue;
      }

      // Try to read schema from file
      String datasetId = getDatasetId(input.getNamespace(), input.getName());
      Optional<List<String>> fileSchema = schemaTracker.tryReadSchemaFromFile(datasetId);

      if (fileSchema.isPresent() && !fileSchema.get().isEmpty()) {
        log.info("Enhanced INPUT dataset {} with file schema: {}", datasetId, fileSchema.get());

        // Create OpenLineage instance
        OpenLineage openLineage = new OpenLineage(event.getProducer());

        // Create schema facet from file schema
        SchemaDatasetFacet schemaFacet = createSchemaFacet(openLineage, fileSchema.get());

        // Build new InputDataset with schema facet
        OpenLineage.DatasetFacetsBuilder facetsBuilder = openLineage.newDatasetFacetsBuilder();

        // Copy existing facets if any
        if (input.getFacets() != null) {
          if (input.getFacets().getDataSource() != null) {
            facetsBuilder.dataSource(input.getFacets().getDataSource());
          }
          if (input.getFacets().getDocumentation() != null) {
            facetsBuilder.documentation(input.getFacets().getDocumentation());
          }
          if (input.getFacets().getLifecycleStateChange() != null) {
            facetsBuilder.lifecycleStateChange(input.getFacets().getLifecycleStateChange());
          }
          if (input.getFacets().getVersion() != null) {
            facetsBuilder.version(input.getFacets().getVersion());
          }
          if (input.getFacets().getColumnLineage() != null) {
            facetsBuilder.columnLineage(input.getFacets().getColumnLineage());
          }
          if (input.getFacets().getStorage() != null) {
            facetsBuilder.storage(input.getFacets().getStorage());
          }
          if (input.getFacets().getSymlinks() != null) {
            facetsBuilder.symlinks(input.getFacets().getSymlinks());
          }
        }

        // Add the schema facet
        facetsBuilder.schema(schemaFacet);

        InputDataset enhancedInput =
            openLineage
                .newInputDatasetBuilder()
                .namespace(input.getNamespace())
                .name(input.getName())
                .facets(facetsBuilder.build())
                .inputFacets(input.getInputFacets())
                .build();

        enhancedInputs.add(enhancedInput);
        anyEnhanced = true;
      } else {
        // No schema available, keep original
        enhancedInputs.add(input);
      }
    }

    // If any inputs were enhanced, rebuild the event
    if (anyEnhanced) {
      log.debug("Rebuilt event with {} enhanced INPUT datasets", enhancedInputs.size());
      return new OpenLineage(event.getProducer())
          .newRunEventBuilder()
          .eventTime(event.getEventTime())
          .eventType(event.getEventType())
          .run(event.getRun())
          .job(event.getJob())
          .inputs(enhancedInputs)
          .outputs(event.getOutputs())
          .build();
    }

    return event;
  }

  /**
   * Create a schema facet from a list of column names.
   *
   * @param openLineage OpenLineage instance
   * @param columnNames list of column names
   * @return schema dataset facet
   */
  private SchemaDatasetFacet createSchemaFacet(OpenLineage openLineage, List<String> columnNames) {
    List<SchemaDatasetFacetFields> fields =
        columnNames.stream()
            .map(
                name ->
                    openLineage
                        .newSchemaDatasetFacetFieldsBuilder()
                        .name(name)
                        .type("string")
                        .build())
            .collect(Collectors.toList());

    return openLineage.newSchemaDatasetFacetBuilder().fields(fields).build();
  }

  /**
   * Track schemas from input and output datasets.
   *
   * @param event the event to track schemas from
   */
  private void trackSchemas(RunEvent event) {
    // Track input schemas (reads)
    if (event.getInputs() != null) {
      for (InputDataset input : event.getInputs()) {
        String datasetId = getDatasetId(input.getNamespace(), input.getName());
        List<String> schema = extractSchema(input.getFacets());
        if (schema != null && !schema.isEmpty()) {
          schemaTracker.recordRead(datasetId, schema);
        }
      }
    }

    // Track output schemas (writes)
    if (event.getOutputs() != null) {
      for (OutputDataset output : event.getOutputs()) {
        String datasetId = getDatasetId(output.getNamespace(), output.getName());
        List<String> schema = extractSchema(output.getFacets());
        if (schema != null && !schema.isEmpty()) {
          schemaTracker.recordWrite(datasetId, schema);
        }
      }
    }
  }

  /**
   * Create a dataset identifier from namespace and name.
   *
   * @param namespace dataset namespace
   * @param name dataset name
   * @return dataset identifier
   */
  private String getDatasetId(String namespace, String name) {
    return namespace + "/" + name;
  }

  /**
   * Extract schema column names from dataset facets.
   *
   * @param facets dataset facets
   * @return list of column names, or null if no schema found
   */
  private List<String> extractSchema(OpenLineage.DatasetFacets facets) {
    if (facets == null || facets.getSchema() == null) {
      return null;
    }

    SchemaDatasetFacet schemaFacet = facets.getSchema();
    if (schemaFacet.getFields() == null) {
      return null;
    }

    return schemaFacet.getFields().stream()
        .map(SchemaDatasetFacetFields::getName)
        .collect(Collectors.toList());
  }

  /**
   * Get the schema tracker (useful for testing).
   *
   * @return schema tracker
   */
  public SchemaHistoryTracker getSchemaTracker() {
    return schemaTracker;
  }

  /**
   * Check if correction is enabled.
   *
   * @return true if enabled
   */
  public boolean isEnabled() {
    return enabled;
  }
}
