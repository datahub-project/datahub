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
package wherehows.dao;

import com.linkedin.events.metadata.DatasetSchema;
import com.linkedin.events.metadata.FieldSchema;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.testng.annotations.Test;
import wherehows.dao.table.FieldDetailDao;
import wherehows.models.table.DictFieldDetail;

import static org.testng.Assert.*;


public class FieldDetailDaoTest {

  @Test
  public void testFillFieldDetailByFieldSchema() {
    FieldDetailDao fieldDetailDao = new FieldDetailDao(null);

    DictFieldDetail field = new DictFieldDetail();

    FieldSchema schema = new FieldSchema();
    schema.fieldPath = "field1";
    schema.position = 1;
    schema.parentFieldPosition = 0;
    schema.type = "string";
    schema.label = "label";
    schema.maxByteLength = 10;
    schema.precision = 1;
    schema.defaultValue = "N/A";
    schema.nullable = false;
    schema.isRecursive = false;

    fieldDetailDao.fillFieldDetailByFieldSchema(schema, field);

    assertEquals(field.getFieldName(), "field1");
    assertEquals(field.getSortId(), 1);
    assertEquals(field.getParentSortId(), 0);
    assertEquals(field.getParentPath(), "");
    assertEquals(field.getFieldsLayoutId(), 0);
    assertEquals(field.getDataType(), "string");
    assertEquals(field.getFieldLabel(), "label");
    assertEquals(field.getDataSize().intValue(), 10);
    assertEquals(field.getDataPrecision().intValue(), 1);
    assertEquals(field.getDefaultValue(), "N/A");
    assertEquals(field.getIsNullable(), "N");
    assertEquals(field.getIsRecursive(), "N");
    assertEquals(field.getModified(), null);
  }

  @Test
  public void testDiffFieldList() {
    FieldDetailDao fieldDetailDao = new FieldDetailDao(null);

    int datasetId = 101;

    DictFieldDetail field1 = new DictFieldDetail();
    field1.setDatasetId(datasetId);
    field1.setFieldName("field1");
    field1.setSortId(1);
    field1.setParentSortId(1);
    field1.setDataType("INTEGER");

    DictFieldDetail field2 = new DictFieldDetail();
    field2.setDatasetId(datasetId);
    field2.setFieldName("field2");
    field2.setSortId(2);
    field2.setParentSortId(1);
    field2.setDataType("STRING");

    List<DictFieldDetail> fields = Arrays.asList(field1, field2);

    DatasetSchema schema = new DatasetSchema();
    FieldSchema fieldSchema1 = new FieldSchema();
    fieldSchema1.fieldPath = "field3";
    fieldSchema1.position = 3;
    fieldSchema1.parentFieldPosition = 1;
    fieldSchema1.type = "LONG";

    FieldSchema fieldSchema2 = new FieldSchema();
    fieldSchema2.fieldPath = "field2";
    fieldSchema2.position = 2;
    fieldSchema2.parentFieldPosition = 1;
    fieldSchema2.type = "LONG";

    schema.fieldSchema = Arrays.asList(fieldSchema1, fieldSchema2);

    List<List<DictFieldDetail>> updatedList = fieldDetailDao.diffFieldList(fields, datasetId, schema);

    List<DictFieldDetail> combined = updatedList.get(0);
    List<DictFieldDetail> removed = updatedList.get(1);

    assertEquals(removed.size(), 1);
    assertEquals(removed.get(0).getFieldName(), "field1");

    assertEquals(combined.size(), 2);
    assertEquals(combined.get(0).getFieldName(), "field3");
    assertEquals(combined.get(0).getDatasetId(), datasetId);
    assertEquals(combined.get(1).getFieldName(), "field2");
  }
}
