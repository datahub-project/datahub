/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.dao.table;

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DatasetProperty;
import com.linkedin.events.metadata.DatasetSchema;
import com.linkedin.events.metadata.FieldSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import wherehows.models.table.DictDataset;
import wherehows.models.table.DictFieldDetail;

import static wherehows.util.UrnUtil.*;


public class FieldDetailDao extends BaseDao {

  private static final String DELETE_BY_DATASET_ID = "DELETE FROM DictFieldDetail WHERE datasetId = :datasetId";

  public FieldDetailDao(@Nonnull EntityManagerFactory factory) {
    super(factory);
  }

  public List<DictFieldDetail> findById(int datasetId) {
    return findListBy(DictFieldDetail.class, "dataset_id", datasetId);
  }

  public void deleteByDatasetId(int datasetId) {
    executeUpdate(DELETE_BY_DATASET_ID, Collections.singletonMap("datasetId", datasetId));
  }

  /**
   * Insert or update dict field details given information from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param dataset DictDataset
   * @param auditStamp ChangeAuditStamp
   * @param schema DatasetSchema
   * @throws Exception
   */
  public void insertUpdateDatasetFields(@Nonnull DatasetIdentifier identifier, @Nullable DictDataset dataset,
      @Nullable DatasetProperty property, @Nonnull ChangeAuditStamp auditStamp, @Nonnull DatasetSchema schema)
      throws Exception {

    if (dataset == null) {
      throw new RuntimeException("Fail to update dataset fields, dataset is NULL.");
    }
    int datasetId = dataset.getId();

    List<DictFieldDetail> fields = findListBy(DictFieldDetail.class, "datasetId", datasetId);

    List<List<DictFieldDetail>> updatedFields = diffFieldList(fields, datasetId, schema);

    if (updatedFields.get(1).size() > 0) {
      removeList(updatedFields.get(1));
    }

    if (updatedFields.get(0).size() > 0) {
      updateList(updatedFields.get(0));
    }

    // update field comments?
  }

  /**
   * Insert or update Schemaless from MetadataChangeEvent
   * @param identifier DatasetIdentifier
   * @param auditStamp ChangeAuditStamp
   * @throws Exception
   */
  public void insertUpdateSchemaless(@Nonnull DatasetIdentifier identifier, @Nonnull ChangeAuditStamp auditStamp)
      throws Exception {
    throw new UnsupportedOperationException("Support for Schemaless not yet implemented.");
  }

  /**
   * Fill in DictFieldDetail information from FieldSchema
   * @param fs FieldSchema
   * @param field DictFieldDetail
   */
  public void fillFieldDetailByFieldSchema(@Nonnull FieldSchema fs, @Nonnull DictFieldDetail field) {
    field.setFieldName(fs.fieldPath.toString());
    field.setSortId(fs.position);
    field.setParentSortId(fs.parentFieldPosition);
    field.setParentPath("");
    field.setFieldsLayoutId(0);
    field.setDataType(trimToLength(toStringOrNull(fs.type), 50)); // truncate to max length 50
    field.setFieldLabel(toStringOrNull(fs.label));
    field.setDataSize(fs.maxByteLength != null ? fs.maxByteLength : fs.maxCharLength);
    field.setDataPrecision(fs.precision);
    field.setDefaultValue(toStringOrNull(fs.defaultValue));
    field.setIsNullable(fs.nullable ? "Y" : "N");
    field.setIsRecursive(fs.isRecursive ? "Y" : "N");
  }

  /**
   * Find the updated list of fields, and the list of fields that don't exist anymore.
   * If new fields are empty, all existing fields will be removed.
   * @param originalFields List<DictFieldDetail>
   * @param datasetId int
   * @param schema DatasetSchema
   * @return [ updated list , removed list of fields]
   */
  public List<List<DictFieldDetail>> diffFieldList(@Nonnull List<DictFieldDetail> originalFields, int datasetId,
      @Nonnull DatasetSchema schema) {
    List<DictFieldDetail> updatedFields = new ArrayList<>(); // updated fields
    List<DictFieldDetail> removedFields = new ArrayList<>(originalFields);  // removed fields

    if (schema.fieldSchema == null || schema.fieldSchema.size() == 0) {
      return Arrays.asList(updatedFields, removedFields);
    }

    int fieldCount = 0;
    for (FieldSchema fs : schema.fieldSchema) {
      fieldCount++;

      String fieldPath = fs.fieldPath.toString();
      // find and update existing field
      for (DictFieldDetail field : originalFields) {
        if (fieldPath.equalsIgnoreCase(field.getFieldName())) {
          fillFieldDetailByFieldSchema(fs, field);

          updatedFields.add(field);
          break;
        }
      }

      // if field not exist, add a new field
      if (updatedFields.size() < fieldCount) {
        DictFieldDetail field = new DictFieldDetail();
        field.setDatasetId(datasetId);
        fillFieldDetailByFieldSchema(fs, field);

        updatedFields.add(field);
      }
    }

    removedFields.removeAll(updatedFields);

    return Arrays.asList(updatedFields, removedFields);
  }
}
