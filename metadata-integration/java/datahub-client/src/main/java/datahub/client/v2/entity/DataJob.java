package datahub.client.v2.entity;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.metadata.aspect.patch.builder.DataJobInfoPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import datahub.client.v2.annotations.RequiresMutable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a DataHub DataJob entity with fluent builder API.
 *
 * <p>A DataJob represents a unit of work in a data processing pipeline (e.g., an Airflow task, a
 * dbt model, a Spark job). DataJobs belong to DataFlows and can have lineage to datasets and other
 * DataJobs.
 *
 * <p>This implementation uses patch-based updates that accumulate until save(). All mutations
 * (addTag, addOwner, setDescription) create patch MCPs rather than modifying aspects directly.
 *
 * <p>Example usage:
 *
 * <pre>
 * DataJob dataJob = DataJob.builder()
 *     .orchestrator("airflow")
 *     .flowId("my_dag")
 *     .cluster("prod")
 *     .jobId("my_task")
 *     .description("Process customer data")
 *     .build();
 *
 * // Add metadata (creates patches)
 * dataJob.addTag("critical");
 * dataJob.addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);
 * dataJob.addTerm("urn:li:glossaryTerm:DataProcessing");
 * dataJob.setDomain("urn:li:domain:Engineering");
 *
 * // Save to DataHub (emits accumulated patches)
 * client.entities().upsert(dataJob);
 * </pre>
 *
 * @see Entity
 */
@Slf4j
public class DataJob extends Entity
    implements HasTags<DataJob>,
        HasGlossaryTerms<DataJob>,
        HasOwners<DataJob>,
        HasDomains<DataJob>,
        HasSubTypes<DataJob>,
        HasStructuredProperties<DataJob> {

  private static final String ENTITY_TYPE = "dataJob";

  /**
   * Constructs a new DataJob entity.
   *
   * @param urn the data job URN
   */
  private DataJob(@Nonnull DataJobUrn urn) {
    super(urn);
  }

  /**
   * Constructs a new DataJob entity from generic URN.
   *
   * @param urn the data job URN (generic type)
   */
  protected DataJob(@Nonnull com.linkedin.common.urn.Urn urn) {
    super(urn);
  }

  /**
   * Constructs a DataJob loaded from server.
   *
   * @param urn the data job URN
   * @param aspects current aspects from server
   */
  private DataJob(
      @Nonnull DataJobUrn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Constructs a DataJob loaded from server with generic URN.
   *
   * @param urn the data job URN (generic type)
   * @param aspects current aspects from server
   */
  protected DataJob(
      @Nonnull com.linkedin.common.urn.Urn urn,
      @Nonnull Map<String, com.linkedin.data.template.RecordTemplate> aspects) {
    super(urn, aspects);
  }

  /**
   * Copy constructor for creating mutable copy via .mutable().
   *
   * @param other the DataJob to copy from
   */
  protected DataJob(@Nonnull DataJob other) {
    super(
        other.urn,
        other.cache,
        other.client,
        other.mode,
        new HashMap<>(),
        new ArrayList<>(),
        new HashMap<>(),
        false,
        false);
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ENTITY_TYPE;
  }

  @Override
  @Nonnull
  public List<Class<? extends com.linkedin.data.template.RecordTemplate>> getDefaultAspects() {
    return List.of(
        com.linkedin.common.Ownership.class,
        com.linkedin.common.GlobalTags.class,
        com.linkedin.common.GlossaryTerms.class,
        com.linkedin.domain.Domains.class,
        com.linkedin.common.Status.class,
        com.linkedin.common.InstitutionalMemory.class,
        DataJobInfo.class,
        com.linkedin.datajob.EditableDataJobProperties.class);
  }

  @Override
  @Nonnull
  public DataJob mutable() {
    if (!readOnly) {
      return this;
    }
    return new DataJob(this);
  }

  /**
   * Returns the data job URN.
   *
   * @return the data job URN
   */
  @Nonnull
  public DataJobUrn getDataJobUrn() {
    return (DataJobUrn) urn;
  }

  /**
   * Creates a new builder for DataJob.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  // ==================== Ownership Operations ====================
  // Provided by HasOwners<DataJob> interface

  // ==================== Tag Operations ====================
  // Provided by HasTags<DataJob> interface

  // ==================== Glossary Term Operations ====================
  // Provided by HasGlossaryTerms<DataJob> interface

  // ==================== Domain Operations ====================
  // Provided by HasDomains<DataJob> interface

  // ==================== Description Operations ====================

  /**
   * Sets the description for this data job.
   *
   * @param description the description
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setDescription(@Nonnull String description) {
    checkNotReadOnly("set description");
    DataJobInfoPatchBuilder patch =
        new DataJobInfoPatchBuilder().urn(getUrn()).setDescription(description);
    addPatchMcp(patch.build());
    log.debug("Added description patch");
    return this;
  }

  /**
   * Gets the description for this data job (lazy-loaded).
   *
   * @return the description, or null if not set
   */
  @Nullable
  public String getDescription() {
    DataJobInfo info = getAspectLazy(DataJobInfo.class);
    return info != null ? info.getDescription() : null;
  }

  // ==================== Display Name Operations ====================

  /**
   * Sets the display name for this data job.
   *
   * @param name the display name
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setName(@Nonnull String name) {
    checkNotReadOnly("set name");
    DataJobInfoPatchBuilder patch = new DataJobInfoPatchBuilder().urn(getUrn()).setName(name);
    addPatchMcp(patch.build());
    log.debug("Added name patch");
    return this;
  }

  /**
   * Gets the display name for this data job (lazy-loaded).
   *
   * @return the display name, or null if not set
   */
  @Nullable
  public String getName() {
    DataJobInfo info = getAspectLazy(DataJobInfo.class);
    return info != null ? info.getName() : null;
  }

  // ==================== Lineage Operations - Input Datasets ====================

  /**
   * Adds an input dataset to this data job.
   *
   * <p>Input datasets represent datasets that this job reads from. This creates lineage from the
   * dataset to the job.
   *
   * @param datasetUrn the dataset URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("add input dataset");
    try {
      DatasetUrn dataset = DatasetUrn.createFromString(datasetUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatasetEdge(dataset);
      addPatchMcp(patch.build());
      log.debug("Added input dataset patch: {}", datasetUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
    return this;
  }

  /**
   * Adds an input dataset to this data job.
   *
   * @param datasetUrn the dataset URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("add input dataset");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatasetEdge(datasetUrn);
    addPatchMcp(patch.build());
    log.debug("Added input dataset patch: {}", datasetUrn);
    return this;
  }

  /**
   * Sets all input datasets for this data job, replacing any existing input datasets.
   *
   * <p>Note: This is a convenience method that adds multiple input datasets. To truly replace all
   * input datasets, you would need to remove existing ones first.
   *
   * @param datasetUrns list of dataset URNs
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setInputDatasets(@Nonnull List<String> datasetUrns) {
    checkNotReadOnly("set input datasets");
    for (String datasetUrn : datasetUrns) {
      addInputDataset(datasetUrn);
    }
    return this;
  }

  /**
   * Removes an input dataset from this data job.
   *
   * @param datasetUrn the dataset URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("remove input dataset");
    try {
      DatasetUrn dataset = DatasetUrn.createFromString(datasetUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatasetEdge(dataset);
      addPatchMcp(patch.build());
      log.debug("Added remove input dataset patch: {}", datasetUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
    return this;
  }

  /**
   * Removes an input dataset from this data job.
   *
   * @param datasetUrn the dataset URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("remove input dataset");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatasetEdge(datasetUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove input dataset patch: {}", datasetUrn);
    return this;
  }

  /**
   * Gets the input datasets for this data job (lazy-loaded).
   *
   * @return list of dataset URNs, or empty list if not set
   */
  @Nonnull
  public List<DatasetUrn> getInputDatasets() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getInputDatasetEdges() == null) {
      return Collections.emptyList();
    }
    return inputOutput.getInputDatasetEdges().stream()
        .map(edge -> (DatasetUrn) edge.getDestinationUrn())
        .collect(Collectors.toList());
  }

  // ==================== Lineage Operations - Output Datasets ====================

  /**
   * Adds an output dataset to this data job.
   *
   * <p>Output datasets represent datasets that this job writes to. This creates lineage from the
   * job to the dataset.
   *
   * @param datasetUrn the dataset URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addOutputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("add output dataset");
    try {
      DatasetUrn dataset = DatasetUrn.createFromString(datasetUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).addOutputDatasetEdge(dataset);
      addPatchMcp(patch.build());
      log.debug("Added output dataset patch: {}", datasetUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
    return this;
  }

  /**
   * Adds an output dataset to this data job.
   *
   * @param datasetUrn the dataset URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addOutputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("add output dataset");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).addOutputDatasetEdge(datasetUrn);
    addPatchMcp(patch.build());
    log.debug("Added output dataset patch: {}", datasetUrn);
    return this;
  }

  /**
   * Sets all output datasets for this data job, replacing any existing output datasets.
   *
   * <p>Note: This is a convenience method that adds multiple output datasets. To truly replace all
   * output datasets, you would need to remove existing ones first.
   *
   * @param datasetUrns list of dataset URNs
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setOutputDatasets(@Nonnull List<String> datasetUrns) {
    checkNotReadOnly("set output datasets");
    for (String datasetUrn : datasetUrns) {
      addOutputDataset(datasetUrn);
    }
    return this;
  }

  /**
   * Removes an output dataset from this data job.
   *
   * @param datasetUrn the dataset URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeOutputDataset(@Nonnull String datasetUrn) {
    checkNotReadOnly("remove output dataset");
    try {
      DatasetUrn dataset = DatasetUrn.createFromString(datasetUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).removeOutputDatasetEdge(dataset);
      addPatchMcp(patch.build());
      log.debug("Added remove output dataset patch: {}", datasetUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URN: " + datasetUrn, e);
    }
    return this;
  }

  /**
   * Removes an output dataset from this data job.
   *
   * @param datasetUrn the dataset URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeOutputDataset(@Nonnull DatasetUrn datasetUrn) {
    checkNotReadOnly("remove output dataset");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).removeOutputDatasetEdge(datasetUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove output dataset patch: {}", datasetUrn);
    return this;
  }

  /**
   * Gets the output datasets for this data job (lazy-loaded).
   *
   * @return list of dataset URNs, or empty list if not set
   */
  @Nonnull
  public List<DatasetUrn> getOutputDatasets() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getOutputDatasetEdges() == null) {
      return Collections.emptyList();
    }
    return inputOutput.getOutputDatasetEdges().stream()
        .map(edge -> (DatasetUrn) edge.getDestinationUrn())
        .collect(Collectors.toList());
  }

  // ==================== Lineage Operations - Input DataJobs (Dependencies) ====================

  /**
   * Adds an input data job dependency to this data job.
   *
   * <p>Input data jobs represent other jobs that this job depends on. This creates lineage from the
   * upstream job to this job, indicating that this job runs after the upstream job completes.
   *
   * @param dataJobUrn the data job URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputDataJob(@Nonnull String dataJobUrn) {
    checkNotReadOnly("add input data job");
    try {
      DataJobUrn dataJob = DataJobUrn.createFromString(dataJobUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatajobEdge(dataJob);
      addPatchMcp(patch.build());
      log.debug("Added input data job patch: {}", dataJobUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid data job URN: " + dataJobUrn, e);
    }
    return this;
  }

  /**
   * Adds an input data job dependency to this data job.
   *
   * @param dataJobUrn the data job URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputDataJob(@Nonnull DataJobUrn dataJobUrn) {
    checkNotReadOnly("add input data job");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatajobEdge(dataJobUrn);
    addPatchMcp(patch.build());
    log.debug("Added input data job patch: {}", dataJobUrn);
    return this;
  }

  /**
   * Sets all input data job dependencies for this data job.
   *
   * <p>Note: This is a convenience method that adds multiple input data jobs. To truly replace all
   * input data jobs, you would need to remove existing ones first.
   *
   * @param dataJobUrns list of data job URNs
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setInputDataJobs(@Nonnull List<String> dataJobUrns) {
    checkNotReadOnly("set input data jobs");
    for (String dataJobUrn : dataJobUrns) {
      addInputDataJob(dataJobUrn);
    }
    return this;
  }

  /**
   * Removes an input data job dependency from this data job.
   *
   * @param dataJobUrn the data job URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputDataJob(@Nonnull String dataJobUrn) {
    checkNotReadOnly("remove input data job");
    try {
      DataJobUrn dataJob = DataJobUrn.createFromString(dataJobUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatajobEdge(dataJob);
      addPatchMcp(patch.build());
      log.debug("Added remove input data job patch: {}", dataJobUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid data job URN: " + dataJobUrn, e);
    }
    return this;
  }

  /**
   * Removes an input data job dependency from this data job.
   *
   * @param dataJobUrn the data job URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputDataJob(@Nonnull DataJobUrn dataJobUrn) {
    checkNotReadOnly("remove input data job");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatajobEdge(dataJobUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove input data job patch: {}", dataJobUrn);
    return this;
  }

  /**
   * Gets the input data job dependencies for this data job (lazy-loaded).
   *
   * @return list of data job URNs, or empty list if not set
   */
  @Nonnull
  public List<DataJobUrn> getInputDataJobs() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getInputDatajobEdges() == null) {
      return Collections.emptyList();
    }
    return inputOutput.getInputDatajobEdges().stream()
        .map(edge -> (DataJobUrn) edge.getDestinationUrn())
        .collect(Collectors.toList());
  }

  // ==================== Lineage Operations - Input/Output Fields ====================

  /**
   * Adds an input field (schema field) to this data job.
   *
   * <p>Input fields represent specific columns from input datasets that this job reads. Format:
   * urn:li:schemaField:(DATASET_URN,COLUMN_NAME)
   *
   * @param fieldUrn the schema field URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputField(@Nonnull String fieldUrn) {
    checkNotReadOnly("add input field");
    try {
      com.linkedin.common.urn.Urn field = com.linkedin.common.urn.Urn.createFromString(fieldUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatasetField(field);
      addPatchMcp(patch.build());
      log.debug("Added input field patch: {}", fieldUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN: " + fieldUrn, e);
    }
    return this;
  }

  /**
   * Adds an input field (schema field) to this data job.
   *
   * @param fieldUrn the schema field URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addInputField(@Nonnull com.linkedin.common.urn.Urn fieldUrn) {
    checkNotReadOnly("add input field");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).addInputDatasetField(fieldUrn);
    addPatchMcp(patch.build());
    log.debug("Added input field patch: {}", fieldUrn);
    return this;
  }

  /**
   * Removes an input field from this data job.
   *
   * @param fieldUrn the schema field URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputField(@Nonnull String fieldUrn) {
    checkNotReadOnly("remove input field");
    try {
      com.linkedin.common.urn.Urn field = com.linkedin.common.urn.Urn.createFromString(fieldUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatasetField(field);
      addPatchMcp(patch.build());
      log.debug("Added remove input field patch: {}", fieldUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN: " + fieldUrn, e);
    }
    return this;
  }

  /**
   * Removes an input field from this data job.
   *
   * @param fieldUrn the schema field URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeInputField(@Nonnull com.linkedin.common.urn.Urn fieldUrn) {
    checkNotReadOnly("remove input field");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).removeInputDatasetField(fieldUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove input field patch: {}", fieldUrn);
    return this;
  }

  /**
   * Gets the input fields for this data job (lazy-loaded).
   *
   * @return list of schema field URNs, or empty list if not set
   */
  @Nonnull
  public List<com.linkedin.common.urn.Urn> getInputFields() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getInputDatasetFields() == null) {
      return Collections.emptyList();
    }
    return new java.util.ArrayList<>(inputOutput.getInputDatasetFields());
  }

  /**
   * Adds an output field (schema field) to this data job.
   *
   * <p>Output fields represent specific columns in output datasets that this job writes. Format:
   * urn:li:schemaField:(DATASET_URN,COLUMN_NAME)
   *
   * @param fieldUrn the schema field URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addOutputField(@Nonnull String fieldUrn) {
    checkNotReadOnly("add output field");
    try {
      com.linkedin.common.urn.Urn field = com.linkedin.common.urn.Urn.createFromString(fieldUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).addOutputDatasetField(field);
      addPatchMcp(patch.build());
      log.debug("Added output field patch: {}", fieldUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN: " + fieldUrn, e);
    }
    return this;
  }

  /**
   * Adds an output field (schema field) to this data job.
   *
   * @param fieldUrn the schema field URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addOutputField(@Nonnull com.linkedin.common.urn.Urn fieldUrn) {
    checkNotReadOnly("add output field");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).addOutputDatasetField(fieldUrn);
    addPatchMcp(patch.build());
    log.debug("Added output field patch: {}", fieldUrn);
    return this;
  }

  /**
   * Removes an output field from this data job.
   *
   * @param fieldUrn the schema field URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeOutputField(@Nonnull String fieldUrn) {
    checkNotReadOnly("remove output field");
    try {
      com.linkedin.common.urn.Urn field = com.linkedin.common.urn.Urn.createFromString(fieldUrn);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder().urn(getUrn()).removeOutputDatasetField(field);
      addPatchMcp(patch.build());
      log.debug("Added remove output field patch: {}", fieldUrn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN: " + fieldUrn, e);
    }
    return this;
  }

  /**
   * Removes an output field from this data job.
   *
   * @param fieldUrn the schema field URN to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeOutputField(@Nonnull com.linkedin.common.urn.Urn fieldUrn) {
    checkNotReadOnly("remove output field");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder().urn(getUrn()).removeOutputDatasetField(fieldUrn);
    addPatchMcp(patch.build());
    log.debug("Added remove output field patch: {}", fieldUrn);
    return this;
  }

  /**
   * Gets the output fields for this data job (lazy-loaded).
   *
   * @return list of schema field URNs, or empty list if not set
   */
  @Nonnull
  public List<com.linkedin.common.urn.Urn> getOutputFields() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getOutputDatasetFields() == null) {
      return Collections.emptyList();
    }
    return new java.util.ArrayList<>(inputOutput.getOutputDatasetFields());
  }

  // ==================== Lineage Operations - Fine-Grained (Column-Level) ====================

  /**
   * Adds fine-grained lineage showing field-to-field transformations.
   *
   * <p>Fine-grained lineage captures column-level transformations, showing which upstream fields
   * are used to derive downstream fields.
   *
   * @param upstreamField upstream schema field URN (format:
   *     urn:li:schemaField:(DATASET_URN,COLUMN_NAME))
   * @param downstreamField downstream schema field URN
   * @param transformationOperation operation type (e.g., "TRANSFORM", "IDENTITY", "AGGREGATION")
   * @param confidenceScore confidence score between 0.0 and 1.0 (defaults to 1.0)
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addFineGrainedLineage(
      @Nonnull String upstreamField,
      @Nonnull String downstreamField,
      @Nonnull String transformationOperation,
      @Nullable Float confidenceScore) {
    checkNotReadOnly("add fine-grained lineage");
    try {
      com.linkedin.common.urn.Urn upstream =
          com.linkedin.common.urn.Urn.createFromString(upstreamField);
      com.linkedin.common.urn.Urn downstream =
          com.linkedin.common.urn.Urn.createFromString(downstreamField);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder()
              .urn(getUrn())
              .addFineGrainedUpstreamField(
                  upstream, confidenceScore, transformationOperation, downstream, null);
      addPatchMcp(patch.build());
      log.debug(
          "Added fine-grained lineage: {} -> {} ({})",
          upstreamField,
          downstreamField,
          transformationOperation);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN", e);
    }
    return this;
  }

  /**
   * Adds fine-grained lineage showing field-to-field transformations.
   *
   * @param upstreamField upstream schema field URN
   * @param downstreamField downstream schema field URN
   * @param transformationOperation operation type (e.g., "TRANSFORM", "IDENTITY", "AGGREGATION")
   * @param confidenceScore confidence score between 0.0 and 1.0 (defaults to 1.0)
   * @param queryUrn optional query URN that this relationship is derived from
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addFineGrainedLineage(
      @Nonnull com.linkedin.common.urn.Urn upstreamField,
      @Nonnull com.linkedin.common.urn.Urn downstreamField,
      @Nonnull String transformationOperation,
      @Nullable Float confidenceScore,
      @Nullable com.linkedin.common.urn.Urn queryUrn) {
    checkNotReadOnly("add fine-grained lineage");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder()
            .urn(getUrn())
            .addFineGrainedUpstreamField(
                upstreamField, confidenceScore, transformationOperation, downstreamField, queryUrn);
    addPatchMcp(patch.build());
    log.debug(
        "Added fine-grained lineage: {} -> {} ({})",
        upstreamField,
        downstreamField,
        transformationOperation);
    return this;
  }

  /**
   * Adds fine-grained lineage with default confidence score of 1.0.
   *
   * @param upstreamField upstream schema field URN
   * @param downstreamField downstream schema field URN
   * @param transformationOperation operation type
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addFineGrainedLineage(
      @Nonnull String upstreamField,
      @Nonnull String downstreamField,
      @Nonnull String transformationOperation) {
    checkNotReadOnly("add fine-grained lineage");
    return addFineGrainedLineage(upstreamField, downstreamField, transformationOperation, null);
  }

  /**
   * Removes fine-grained lineage.
   *
   * @param upstreamField upstream schema field URN
   * @param downstreamField downstream schema field URN
   * @param transformationOperation operation type
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeFineGrainedLineage(
      @Nonnull String upstreamField,
      @Nonnull String downstreamField,
      @Nonnull String transformationOperation) {
    checkNotReadOnly("remove fine-grained lineage");
    try {
      com.linkedin.common.urn.Urn upstream =
          com.linkedin.common.urn.Urn.createFromString(upstreamField);
      com.linkedin.common.urn.Urn downstream =
          com.linkedin.common.urn.Urn.createFromString(downstreamField);
      DataJobInputOutputPatchBuilder patch =
          new DataJobInputOutputPatchBuilder()
              .urn(getUrn())
              .removeFineGrainedUpstreamField(upstream, transformationOperation, downstream, null);
      addPatchMcp(patch.build());
      log.debug(
          "Added remove fine-grained lineage: {} -> {} ({})",
          upstreamField,
          downstreamField,
          transformationOperation);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid field URN", e);
    }
    return this;
  }

  /**
   * Removes fine-grained lineage.
   *
   * @param upstreamField upstream schema field URN
   * @param downstreamField downstream schema field URN
   * @param transformationOperation operation type
   * @param queryUrn optional query URN
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeFineGrainedLineage(
      @Nonnull com.linkedin.common.urn.Urn upstreamField,
      @Nonnull com.linkedin.common.urn.Urn downstreamField,
      @Nonnull String transformationOperation,
      @Nullable com.linkedin.common.urn.Urn queryUrn) {
    checkNotReadOnly("remove fine-grained lineage");
    DataJobInputOutputPatchBuilder patch =
        new DataJobInputOutputPatchBuilder()
            .urn(getUrn())
            .removeFineGrainedUpstreamField(
                upstreamField, transformationOperation, downstreamField, queryUrn);
    addPatchMcp(patch.build());
    log.debug(
        "Added remove fine-grained lineage: {} -> {} ({})",
        upstreamField,
        downstreamField,
        transformationOperation);
    return this;
  }

  /**
   * Gets the fine-grained lineages for this data job (lazy-loaded).
   *
   * @return list of fine-grained lineage objects, or empty list if not set
   */
  @Nonnull
  public List<com.linkedin.dataset.FineGrainedLineage> getFineGrainedLineages() {
    DataJobInputOutput inputOutput = getAspectLazy(DataJobInputOutput.class);
    if (inputOutput == null || inputOutput.getFineGrainedLineages() == null) {
      return Collections.emptyList();
    }
    return new java.util.ArrayList<>(inputOutput.getFineGrainedLineages());
  }

  // ==================== Custom Properties Operations ====================

  /**
   * Adds a custom property to this data job.
   *
   * @param key the property key
   * @param value the property value
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob addCustomProperty(@Nonnull String key, @Nonnull String value) {
    checkNotReadOnly("add custom property");
    DataJobInfoPatchBuilder builder = getPatchBuilder("dataJobInfo", DataJobInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DataJobInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dataJobInfo", builder);
    }

    builder.addCustomProperty(key, value);
    log.debug("Added custom property to accumulated builder: {}={}", key, value);
    return this;
  }

  /**
   * Removes a custom property from this data job.
   *
   * @param key the property key to remove
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob removeCustomProperty(@Nonnull String key) {
    checkNotReadOnly("remove custom property");
    DataJobInfoPatchBuilder builder = getPatchBuilder("dataJobInfo", DataJobInfoPatchBuilder.class);
    if (builder == null) {
      builder = new DataJobInfoPatchBuilder().urn(getUrn());
      registerPatchBuilder("dataJobInfo", builder);
    }

    builder.removeCustomProperty(key);
    log.debug("Added remove custom property to accumulated builder: {}", key);
    return this;
  }

  /**
   * Sets custom properties for this data job (replaces all).
   *
   * @param properties map of custom properties
   * @return this data job
   */
  @RequiresMutable
  @Nonnull
  public DataJob setCustomProperties(@Nonnull Map<String, String> properties) {
    checkNotReadOnly("set custom properties");
    DataJobInfoPatchBuilder patch =
        new DataJobInfoPatchBuilder().urn(getUrn()).setCustomProperties(properties);
    addPatchMcp(patch.build());
    log.debug("Added set custom properties patch with {} properties", properties.size());
    return this;
  }

  // ==================== Builder ====================

  /** Builder for creating DataJob entities with a fluent API. */
  public static class Builder {
    private String orchestrator;
    private String flowId;
    private String cluster = "prod";
    private String jobId;
    private String description;
    private String name;
    private String type;
    private Map<String, String> customProperties;

    /**
     * Sets the orchestrator (required).
     *
     * @param orchestrator the orchestrator (e.g., "airflow", "dagster", "prefect")
     * @return this builder
     */
    @Nonnull
    public Builder orchestrator(@Nonnull String orchestrator) {
      this.orchestrator = orchestrator;
      return this;
    }

    /**
     * Sets the flow ID (required).
     *
     * @param flowId the flow/DAG ID
     * @return this builder
     */
    @Nonnull
    public Builder flowId(@Nonnull String flowId) {
      this.flowId = flowId;
      return this;
    }

    /**
     * Sets the cluster name. Default is "prod".
     *
     * @param cluster the cluster name (e.g., "prod", "dev", "staging")
     * @return this builder
     */
    @Nonnull
    public Builder cluster(@Nonnull String cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the job ID (required).
     *
     * @param jobId the job/task ID
     * @return this builder
     */
    @Nonnull
    public Builder jobId(@Nonnull String jobId) {
      this.jobId = jobId;
      return this;
    }

    /**
     * Sets the description.
     *
     * @param description the description
     * @return this builder
     */
    @Nonnull
    public Builder description(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the display name.
     *
     * @param name the display name
     * @return this builder
     */
    @Nonnull
    public Builder name(@Nullable String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the job type (required if creating DataJobInfo aspect).
     *
     * <p>Examples: "BATCH", "STREAMING", "SPARK", "SQL"
     *
     * @param type the job type
     * @return this builder
     */
    @Nonnull
    public Builder type(@Nullable String type) {
      this.type = type;
      return this;
    }

    /**
     * Sets custom properties.
     *
     * @param properties map of custom properties
     * @return this builder
     */
    @Nonnull
    public Builder customProperties(@Nullable Map<String, String> properties) {
      this.customProperties = properties;
      return this;
    }

    /**
     * Sets the URN for this datajob by extracting its components.
     *
     * @param urn the datajob URN
     * @return this builder
     */
    @Nonnull
    public Builder urn(@Nonnull DataJobUrn urn) {
      DataFlowUrn flowUrn = urn.getFlowEntity();
      this.orchestrator = flowUrn.getOrchestratorEntity();
      this.flowId = flowUrn.getFlowIdEntity();
      this.cluster = flowUrn.getClusterEntity();
      this.jobId = urn.getJobIdEntity();
      return this;
    }

    /**
     * Builds the DataJob entity.
     *
     * <p>Note: Properties set in the builder are cached as aspects, not converted to patches. Use
     * the setter methods on the DataJob after building if you want patch-based updates.
     *
     * @return the DataJob entity
     * @throws IllegalArgumentException if required fields are missing
     */
    @Nonnull
    public DataJob build() {
      if (orchestrator == null || flowId == null || jobId == null) {
        throw new IllegalArgumentException("orchestrator, flowId, and jobId are required");
      }

      try {
        DataFlowUrn flowUrn = new DataFlowUrn(orchestrator, flowId, cluster);
        DataJobUrn urn = new DataJobUrn(flowUrn, jobId);
        DataJob dataJob = new DataJob(urn);

        // Cache DataJobInfo aspect only if we have required fields
        // Following Stripe's pattern: required fields are only required when creating the aspect
        boolean wantsToCreateInfo =
            description != null || name != null || type != null || customProperties != null;

        if (wantsToCreateInfo) {
          // DataJobInfo requires both 'name' and 'type' fields
          if (name == null || type == null) {
            throw new IllegalArgumentException(
                "DataJobInfo aspect requires both 'name' and 'type' fields. "
                    + "Provide both .name() and .type() in builder, "
                    + "or don't set description/customProperties (use addTag/addOwner instead).");
          }

          DataJobInfo info = new DataJobInfo();
          info.setName(name);
          info.setType(DataJobInfo.Type.create(type));
          if (description != null) {
            info.setDescription(description);
          }
          if (customProperties != null) {
            info.setCustomProperties(new StringMap(customProperties));
          }
          String aspectName = dataJob.getAspectName(DataJobInfo.class);
          dataJob.cache.put(aspectName, info, AspectSource.LOCAL, true);
        }

        return dataJob;
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to create data job URN", e);
      }
    }
  }
}
