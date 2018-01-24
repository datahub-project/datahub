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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.events.metadata.CaseSensitivityInfo;
import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.DatasetProperty;
import com.linkedin.events.metadata.DatasetRefresh;
import com.linkedin.events.metadata.DatasetSchema;
import com.linkedin.events.metadata.DatasetStorageType;
import com.linkedin.events.metadata.PartitionKey;
import com.linkedin.events.metadata.PartitionSpecification;
import com.linkedin.events.metadata.PartitionType;
import com.linkedin.events.metadata.PlatformNativeType;
import com.linkedin.events.metadata.RawSchema;
import com.linkedin.events.metadata.SchemaTextFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;
import wherehows.dao.table.DictDatasetDao;
import wherehows.models.table.DictDataset;

import static org.testng.Assert.*;


public class DictDatasetDaoTest {

  @Test
  public void testFillDictDatasetEmpty() throws Exception {
    DictDatasetDao dictDatasetDao = new DictDatasetDao(null);

    DictDataset ds = new DictDataset();
    String urn = "oracle:///abc/test";

    ChangeAuditStamp auditStamp = new ChangeAuditStamp();
    Long testTime = System.currentTimeMillis();
    auditStamp.actorUrn = "urn:li:user:tester";
    auditStamp.time = testTime;

    dictDatasetDao.fillDictDataset(ds, urn, auditStamp, null, null, null, null, null, null);

    assertEquals(ds.getUrn(), urn);
    assertEquals(ds.getSource(), "oracle");
    assertTrue(ds.getIsActive());

    DatasetSchema schema = new DatasetSchema();
    schema.rawSchema = new RawSchema();
    schema.rawSchema.content = "Raw Schema";
    schema.rawSchema.format = SchemaTextFormat.TEXT;
    schema.fieldSchema = new ArrayList<>();

    dictDatasetDao.fillDictDataset(ds, urn, auditStamp, null, schema, null, null, null, null);

    assertEquals(ds.getSourceCreatedTime().intValue(), testTime / 1000);
    assertEquals(ds.getSourceModifiedTime().intValue(), testTime / 1000);
    assertEquals(ds.getCreatedTime(), null);
    assertEquals(ds.getModifiedTime(), null);

    assertEquals(ds.getSchema(), "Raw Schema");
    assertEquals(ds.getSchemaType(), "TEXT");
    assertEquals(ds.getFields(), "[]");
    assertEquals(ds.getProperties(), "{}");

    DatasetProperty property = new DatasetProperty();
    property.nativeType = PlatformNativeType.TABLE;
    property.storageType = DatasetStorageType.TABLE;
    property.uri = urn;
    property.caseSensitivity = new CaseSensitivityInfo();
    property.caseSensitivity.dataContent = true;
    property.extras = Collections.singletonMap("foo", "bar");

    List<String> tags = Arrays.asList("tag1", "tag2");

    PartitionSpecification partitions = new PartitionSpecification();
    PartitionKey partitionKey = new PartitionKey();
    partitionKey.partitionLevel = 1;
    partitionKey.fieldNames = Arrays.asList("Partition");
    partitionKey.partitionType = PartitionType.HASH;
    partitions.totalPartitionLevel = 1;
    partitions.hasHashPartition = true;
    partitions.partitionKeys = Arrays.asList(partitionKey);

    dictDatasetDao.fillDictDataset(ds, urn, auditStamp, property, schema, new ArrayList<>(), tags, new ArrayList<>(),
        partitions);

    Map<String, Object> properties =
        new ObjectMapper().readValue(ds.getProperties(), new TypeReference<Map<String, Object>>() {
        });

    String propertyStr = "{nativeType: TABLE, storageType: TABLE, uri: oracle:///abc/test, caseSensitivity: "
        + "{datasetName: false, fieldName: false, dataContent: true}, refresh: null, extras: {foo: bar}}";

    String partitionStr = "{totalPartitionLevel: 1, partitionSpecText: null, hasTimePartition: null, "
        + "hasHashPartition: true, partitionKeys: [{partitionLevel: 1, partitionType: HASH, timeFormat: null, "
        + "fieldNames: [Partition], partitionValues: null, numberOfHashBuckets: null}]}";

    assertEquals(properties.get("property").toString().replaceAll("\"", ""), propertyStr);
    assertEquals(properties.get("tag"), Arrays.asList("tag1", "tag2"));
    assertEquals(properties.get("deployment"), "[]");
    assertEquals(properties.get("capacity"), "[]");
    assertEquals(properties.get("partition").toString().replaceAll("\"", ""), partitionStr);
  }

  @Test
  public void testFillDictDatasetExist() throws IOException {
    DictDatasetDao dictDatasetDao = new DictDatasetDao(null);

    DictDataset ds = new DictDataset();
    String urn = "oracle:///abc/test";

    ChangeAuditStamp auditStamp = new ChangeAuditStamp();
    Long testTime1 = System.currentTimeMillis();
    auditStamp.actorUrn = "urn:li:user:tester";
    auditStamp.time = testTime1;

    DatasetSchema schema = new DatasetSchema();
    schema.rawSchema = new RawSchema();
    schema.rawSchema.content = "Raw Schema";
    schema.rawSchema.format = SchemaTextFormat.TEXT;
    schema.fieldSchema = new ArrayList<>();

    DatasetProperty property1 = new DatasetProperty();
    property1.nativeType = PlatformNativeType.TABLE;
    property1.storageType = DatasetStorageType.INDEX;

    List<String> tags1 = Arrays.asList("tag1", "tag2");

    dictDatasetDao.fillDictDataset(ds, urn, auditStamp, property1, schema, new ArrayList<>(), tags1, null, null);

    Long testTime2 = System.currentTimeMillis();
    auditStamp.time = testTime2;

    DatasetProperty property2 = new DatasetProperty();
    property2.nativeType = PlatformNativeType.TABLE;
    property2.storageType = DatasetStorageType.TABLE;
    property2.refresh = new DatasetRefresh();
    property2.refresh.lastRefresh = 10000;

    List<String> tags2 = Arrays.asList("tag1", "tag3");

    PartitionSpecification partitions = new PartitionSpecification();
    PartitionKey partitionKey = new PartitionKey();
    partitionKey.partitionLevel = 1;
    partitionKey.fieldNames = Arrays.asList("Partition");
    partitionKey.partitionType = PartitionType.HASH;
    partitions.totalPartitionLevel = 1;
    partitions.hasHashPartition = true;
    partitions.partitionKeys = Arrays.asList(partitionKey);

    dictDatasetDao.fillDictDataset(ds, urn, auditStamp, property2, schema, null, tags2, null, partitions);

    assertEquals(ds.getSourceCreatedTime().intValue(), testTime1 / 1000);
    assertEquals(ds.getSourceModifiedTime().intValue(), testTime2 / 1000);

    Map<String, Object> properties =
        new ObjectMapper().readValue(ds.getProperties(), new TypeReference<Map<String, Object>>() {
        });

    String propertyStr =
        "{nativeType: TABLE, storageType: TABLE, uri: null, caseSensitivity: null, refresh: {lastRefresh: 10000}, extras: null}";

    String partitionStr = "{totalPartitionLevel: 1, partitionSpecText: null, hasTimePartition: null, "
        + "hasHashPartition: true, partitionKeys: [{partitionLevel: 1, partitionType: HASH, timeFormat: null, "
        + "fieldNames: [Partition], partitionValues: null, numberOfHashBuckets: null}]}";

    assertEquals(properties.get("property").toString().replaceAll("\"", ""), propertyStr);
    assertEquals(properties.get("tag"), Arrays.asList("tag1", "tag3"));
    assertEquals(properties.get("deployment"), "[]");
    assertTrue(!properties.containsKey("capacity"));
    assertEquals(properties.get("partition").toString().replaceAll("\"", ""), partitionStr);
  }
}
