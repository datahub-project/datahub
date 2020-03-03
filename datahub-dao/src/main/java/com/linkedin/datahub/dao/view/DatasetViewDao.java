package com.linkedin.datahub.dao.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.models.view.DatasetColumn;
import com.linkedin.datahub.models.view.DatasetSchema;
import com.linkedin.datahub.models.view.DatasetView;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.dataset.client.Deprecations;
import com.linkedin.dataset.client.Schemas;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.datahub.util.DatasetUtil.toDatasetUrn;
import static com.linkedin.datahub.util.DatasetUtil.toDatasetView;
import static com.linkedin.datahub.util.RestliUtil.toJsonNode;


@Slf4j
public class DatasetViewDao {

  private final Datasets _datasets;
  private final Deprecations _deprecations;
  private final com.linkedin.dataset.client.InstitutionalMemory _institutionalMemory;
  private final Schemas _schemas;

  public DatasetViewDao(@Nonnull Datasets datasets,
                        @Nonnull Deprecations deprecations,
                        @Nonnull com.linkedin.dataset.client.InstitutionalMemory institutionalMemory,
                        @Nonnull Schemas schemas) {
    this._datasets = datasets;
    this._deprecations = deprecations;
    this._institutionalMemory = institutionalMemory;
    this._schemas = schemas;
  }

  @Nonnull
  public DatasetView getDatasetView(@Nonnull String datasetUrn) throws Exception {
    return toDatasetView(_datasets.get(toDatasetUrn(datasetUrn)));
  }

  @Nonnull
  public JsonNode getSnapshot(@Nonnull String datasetUrn) throws Exception {
    DatasetSnapshot snapshot = _datasets.getLatestFullSnapshot(toDatasetUrn(datasetUrn));
    return toJsonNode(snapshot);
  }

  public void updateInstitutionalMemory(@Nonnull String datasetUrn, @Nonnull InstitutionalMemory institutionalMemory)
      throws Exception {
    _institutionalMemory.updateInstitutionalMemory(toDatasetUrn(datasetUrn), institutionalMemory);
  }

  @Nonnull
  public JsonNode getInstitutionalMemory(@Nonnull String datasetUrn) throws Exception {
    InstitutionalMemory institutionalMemory = _institutionalMemory.getInstitutionalMemory(toDatasetUrn(datasetUrn));
    return toJsonNode(institutionalMemory);
  }

  public void setDatasetDeprecation(@Nonnull String datasetUrn, boolean isDeprecated, @Nonnull String deprecationNote,
      @Nullable Long decommissionTime, @Nonnull String user) throws Exception {
    DatasetDeprecation datasetDeprecation = new DatasetDeprecation()
        .setDeprecated(isDeprecated)
        .setNote(deprecationNote)
        .setActor(new CorpuserUrn(user));
    if (decommissionTime != null) {
      datasetDeprecation.setDecommissionTime(decommissionTime);
    }
    _deprecations.updateDatasetDeprecation(toDatasetUrn(datasetUrn), datasetDeprecation);
  }

  /**
   * Get dataset schema by dataset urn
   * @param datasetUrn String
   * @return dataset schema
   */
  @Nullable
  public DatasetSchema getDatasetSchema(@Nonnull String datasetUrn) throws Exception {
    SchemaMetadata schema = _schemas.getLatestSchemaByDataset(toDatasetUrn(datasetUrn));
    if (schema == null) {
      return null;
    }
    SchemaMetadata.PlatformSchema platformSchema = schema.getPlatformSchema();

    DatasetSchema dsSchema = new DatasetSchema();
    dsSchema.setLastModified(schema.getLastModified().getTime());
    if (platformSchema.isSchemaless()) {
      dsSchema.setSchemaless(true);
      dsSchema.setColumns(new ArrayList<>());
      return dsSchema;
    }

    if (platformSchema.isPrestoDDL()) {
      dsSchema.setRawSchema(platformSchema.getPrestoDDL().getRawSchema());
    } else if (platformSchema.isOracleDDL()) {
      dsSchema.setRawSchema(platformSchema.getOracleDDL().getTableSchema());
    } else if (platformSchema.isMySqlDDL()) {
      dsSchema.setRawSchema(platformSchema.getMySqlDDL().getTableSchema());
    } else if (platformSchema.isKafkaSchema()) {
      dsSchema.setRawSchema(platformSchema.getKafkaSchema().getDocumentSchema());
    } else if (platformSchema.isOrcSchema()) {
      dsSchema.setRawSchema(platformSchema.getOrcSchema().getSchema());
    } else if (platformSchema.isBinaryJsonSchema()) {
      dsSchema.setRawSchema(platformSchema.getBinaryJsonSchema().getSchema());
    } else if (platformSchema.isEspressoSchema()) {
      dsSchema.setKeySchema(platformSchema.getEspressoSchema().getTableSchema());
      dsSchema.setRawSchema(platformSchema.getEspressoSchema().getDocumentSchema());
    } else if (platformSchema.isKeyValueSchema()) {
      dsSchema.setKeySchema(platformSchema.getKeyValueSchema().getKeySchema());
      dsSchema.setRawSchema(platformSchema.getKeyValueSchema().getValueSchema());
    }
    dsSchema.setSchemaless(false);

    if (schema.hasFields() && schema.getFields().size() > 0) {
      dsSchema.setColumns(toWhDatasetColumns(schema.getFields()));
    }
    return dsSchema;
  }

  /**
   * Convert TMS schema field array to WH list of DatasetColumn
   */
  private List<DatasetColumn> toWhDatasetColumns(@Nonnull SchemaFieldArray fields) {
    List<DatasetColumn> columns = new ArrayList<>();
    for (SchemaField field : fields) {
      DatasetColumn col = new DatasetColumn();
      col.setFieldName(field.getFieldPath());
      col.setFullFieldPath(field.getFieldPath());
      col.setDataType(field.hasNativeDataType() ? field.getNativeDataType() : "");
      col.setComment(field.hasDescription() ? field.getDescription() : "");
      columns.add(col);
    }
    return columns;
  }
}
