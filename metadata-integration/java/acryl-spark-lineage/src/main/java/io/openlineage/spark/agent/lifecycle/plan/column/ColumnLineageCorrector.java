/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.column;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFields;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.column.SchemaHistoryTracker.SchemaMismatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Corrects column lineage for RDD conversions using positional mapping.
 *
 * <p>When a DataFrame is converted to RDD and back, the logical plan is lost. This results in
 * self-referential column lineage (e.g., cust_id ← cust_id) instead of tracking the actual
 * transformations (e.g., cust_id ← id).
 *
 * <p>This corrector uses schema history to detect such cases and applies positional mapping to fix
 * the lineage.
 */
@Slf4j
public class ColumnLineageCorrector {

  private final SchemaHistoryTracker schemaTracker;
  private final OpenLineage openLineage;

  public ColumnLineageCorrector(SchemaHistoryTracker schemaTracker) {
    this.schemaTracker = schemaTracker;
    this.openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  }

  /**
   * Correct column lineage in a RunEvent if schema mismatches are detected.
   *
   * @param event the event to correct
   * @return corrected event (may be the same instance if no corrections needed)
   */
  public RunEvent correctLineage(RunEvent event) {
    if (event.getOutputs() == null || event.getOutputs().isEmpty()) {
      return event;
    }

    boolean correctionApplied = false;
    List<OutputDataset> correctedOutputs = new ArrayList<>();

    for (OutputDataset output : event.getOutputs()) {
      OutputDataset correctedOutput = correctOutputDataset(output);
      correctedOutputs.add(correctedOutput);
      if (correctedOutput != output) {
        correctionApplied = true;
      }
    }

    if (correctionApplied) {
      log.info("Applied RDD lineage correction to event for run {}", event.getRun().getRunId());
      // Create a new RunEvent with corrected outputs using builder
      return openLineage
          .newRunEventBuilder()
          .eventTime(event.getEventTime())
          .eventType(event.getEventType())
          .run(event.getRun())
          .job(event.getJob())
          .inputs(event.getInputs())
          .outputs(correctedOutputs)
          .build();
    }

    return event;
  }

  /**
   * Correct column lineage for a single output dataset.
   *
   * @param output the output dataset
   * @return corrected output dataset (may be the same instance if no corrections needed)
   */
  private OutputDataset correctOutputDataset(OutputDataset output) {
    if (output.getFacets() == null || output.getFacets().getColumnLineage() == null) {
      return output;
    }

    ColumnLineageDatasetFacet columnLineage = output.getFacets().getColumnLineage();
    ColumnLineageDatasetFacetFields fields = columnLineage.getFields();

    if (fields == null || fields.getAdditionalProperties() == null) {
      return output;
    }

    // Check if any input fields reference datasets with schema mismatches
    Map<String, SchemaMismatch> mismatches =
        detectMismatchesInLineage(fields.getAdditionalProperties());

    if (mismatches.isEmpty()) {
      return output; // No corrections needed
    }

    // Apply corrections
    ColumnLineageDatasetFacetFields correctedFields =
        applyCorrections(fields.getAdditionalProperties(), mismatches);

    // Create new facets with corrected lineage
    ColumnLineageDatasetFacet correctedLineage =
        openLineage.newColumnLineageDatasetFacetBuilder().fields(correctedFields).build();

    OpenLineage.DatasetFacets correctedFacets =
        openLineage
            .newDatasetFacetsBuilder()
            .columnLineage(correctedLineage)
            .dataSource(output.getFacets().getDataSource())
            .schema(output.getFacets().getSchema())
            .documentation(output.getFacets().getDocumentation())
            .lifecycleStateChange(output.getFacets().getLifecycleStateChange())
            .ownership(output.getFacets().getOwnership())
            .storage(output.getFacets().getStorage())
            .version(output.getFacets().getVersion())
            .build();

    return openLineage
        .newOutputDatasetBuilder()
        .namespace(output.getNamespace())
        .name(output.getName())
        .facets(correctedFacets)
        .outputFacets(output.getOutputFacets())
        .build();
  }

  /**
   * Detect schema mismatches in column lineage input fields.
   *
   * @param fieldsMap map of field name to ColumnLineageDatasetFacetFieldsAdditional
   * @return map of dataset identifier to schema mismatch
   */
  private Map<String, SchemaMismatch> detectMismatchesInLineage(
      Map<String, ColumnLineageDatasetFacetFieldsAdditional> fieldsMap) {

    Map<String, SchemaMismatch> mismatches = new HashMap<>();

    for (ColumnLineageDatasetFacetFieldsAdditional fieldAdditional : fieldsMap.values()) {
      if (fieldAdditional.getInputFields() == null) {
        continue;
      }

      for (InputField inputField : fieldAdditional.getInputFields()) {
        String datasetId = inputField.getNamespace() + "/" + inputField.getName();

        // Check if we've already detected a mismatch for this dataset
        if (mismatches.containsKey(datasetId)) {
          continue;
        }

        // Check for schema mismatch
        Optional<SchemaMismatch> mismatch = schemaTracker.detectMismatch(datasetId);
        if (mismatch.isPresent()) {
          mismatches.put(datasetId, mismatch.get());
          log.debug("Detected schema mismatch for dataset {}", datasetId);
        }
      }
    }

    return mismatches;
  }

  /**
   * Apply positional mapping corrections to column lineage fields.
   *
   * @param fieldsMap original fields map
   * @param mismatches detected schema mismatches
   * @return corrected fields
   */
  private ColumnLineageDatasetFacetFields applyCorrections(
      Map<String, ColumnLineageDatasetFacetFieldsAdditional> fieldsMap,
      Map<String, SchemaMismatch> mismatches) {

    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        openLineage.newColumnLineageDatasetFacetFieldsBuilder();

    for (Map.Entry<String, ColumnLineageDatasetFacetFieldsAdditional> entry :
        fieldsMap.entrySet()) {
      String outputColumn = entry.getKey();
      ColumnLineageDatasetFacetFieldsAdditional fieldAdditional = entry.getValue();

      if (fieldAdditional.getInputFields() == null) {
        fieldsBuilder.put(outputColumn, fieldAdditional);
        continue;
      }

      List<InputField> correctedInputFields = new ArrayList<>();
      boolean fieldCorrected = false;

      for (InputField inputField : fieldAdditional.getInputFields()) {
        String datasetId = inputField.getNamespace() + "/" + inputField.getName();
        SchemaMismatch mismatch = mismatches.get(datasetId);

        if (mismatch != null) {
          // Apply positional mapping
          Map<String, String> mapping = mismatch.createPositionalMapping();
          String correctedField = mapping.get(inputField.getField());

          if (correctedField != null && !correctedField.equals(inputField.getField())) {
            log.info(
                "Corrected RDD lineage: {} ← {} (was: {})",
                outputColumn,
                correctedField,
                inputField.getField());

            // Create corrected input field
            InputField correctedInputField =
                openLineage
                    .newInputFieldBuilder()
                    .namespace(inputField.getNamespace())
                    .name(inputField.getName())
                    .field(correctedField)
                    .transformations(inputField.getTransformations())
                    .build();

            correctedInputFields.add(correctedInputField);
            fieldCorrected = true;
          } else {
            correctedInputFields.add(inputField);
          }
        } else {
          correctedInputFields.add(inputField);
        }
      }

      if (fieldCorrected) {
        ColumnLineageDatasetFacetFieldsAdditional correctedAdditional =
            openLineage
                .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                .inputFields(correctedInputFields)
                .build();
        fieldsBuilder.put(outputColumn, correctedAdditional);
      } else {
        fieldsBuilder.put(outputColumn, fieldAdditional);
      }
    }

    return fieldsBuilder.build();
  }
}
