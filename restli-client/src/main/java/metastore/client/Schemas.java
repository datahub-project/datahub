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
package metastore.client;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.EspressoSchema;
import com.linkedin.dataset.ForeignKeySpecMap;
import com.linkedin.dataset.KafkaSchema;
import com.linkedin.dataset.OracleDDL;
import com.linkedin.dataset.SchemaField;
import com.linkedin.dataset.SchemaFieldArray;
import com.linkedin.dataset.SchemaFieldDataType;
import com.linkedin.dataset.SchemaMetadata;
import com.linkedin.dataset.SchemaMetadataFindByDatasetRequestBuilder;
import com.linkedin.dataset.SchemaMetadataGetRequestBuilder;
import com.linkedin.dataset.SchemaMetadataKey;
import com.linkedin.dataset.SchemaMetadataRequestBuilders;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.util.List;

import static metastore.util.UrnUtil.*;


public class Schemas {

  private final RestClient _client;

  private static final SchemaMetadataRequestBuilders _schemaMetadataBuilder = new SchemaMetadataRequestBuilders();

  public Schemas(RestClient metadataStoreClient) {
    _client = metadataStoreClient;
  }

  public SchemaMetadata getSchemaMetadata(String platformName, String schemaName, long version) throws Exception {

    SchemaMetadataKey key = new SchemaMetadataKey().setSchemaName(schemaName)
        .setPlatform(toDataPlatformUrn(platformName))
        .setVersion(version);

    SchemaMetadataGetRequestBuilder builder = _schemaMetadataBuilder.get();
    Request<SchemaMetadata> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    ResponseFuture<SchemaMetadata> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public SchemaMetadata getLatestSchemaByDataset(String platformName, String datasetName, String origin)
      throws Exception {
    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);

    SchemaMetadataFindByDatasetRequestBuilder builder = _schemaMetadataBuilder.findByDataset();
    FindRequest<SchemaMetadata> req = builder.datasetParam(urn).build();

    ResponseFuture<CollectionResponse<SchemaMetadata>> responseFuture = _client.sendRequest(req);
    long version = 0;
    SchemaMetadata latestSchema = null;
    for (SchemaMetadata sc : responseFuture.getResponse().getEntity().getElements()) {
      if (sc.getVersion() > version) {
        latestSchema = sc;
        version = sc.getVersion();
      }
    }
    return latestSchema;
  }

  public SchemaMetadataKey createSchemaMetadataEspresso(String platformName, String schemaName, String datasetName,
      String origin, String tableSchema, String docSchema, Urn actor) throws Exception {

    EspressoSchema schema = new EspressoSchema().setTableSchema(tableSchema).setDocumentSchema(docSchema);
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setEspressoSchema(schema);

    return createSchemaMetadata(platformName, schemaName, datasetName, origin, platformSchema, new SchemaFieldArray(),
        actor);
  }

  public SchemaMetadataKey createSchemaMetadataKafka(String platformName, String schemaName, String datasetName,
      String origin, String docSchema, Urn actor) throws Exception {

    KafkaSchema schema = new KafkaSchema().setDocumentSchema(docSchema);
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setKafkaSchema(schema);

    return createSchemaMetadata(platformName, schemaName, datasetName, origin, platformSchema, new SchemaFieldArray(),
        actor);
  }

  public SchemaMetadataKey createSchemaMetadataOracle(String platformName, String schemaName, String datasetName,
      String origin, List<Object[]> fieldInfo, Urn actor) throws Exception {

    OracleDDL ddl = new OracleDDL().setTableSchema("");
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setOracleDDL(ddl);

    SchemaFieldArray fields = new SchemaFieldArray();
    for (Object[] fd : fieldInfo) {
      // sort_id, field_name, data_type, is_nullable, is_recursive, modified
      String fieldname = fd[1].toString();
      SchemaFieldDataType dataType = toFieldDataType(fd[2].toString());
      Boolean nullable = fd[3] != null && 'Y' == (char) fd[3];
      Boolean recursive = fd[4] != null && 'Y' == (char) fd[4];
      String comment = String.valueOf(fd[5]);

      fields.add(new SchemaField().setFieldPath(fieldname)
          .setType(dataType)
          .setNullable(nullable)
          .setRecursive(recursive)
          .setDescription(comment));
    }
    //System.out.println(fields);

    return createSchemaMetadata(platformName, schemaName, datasetName, origin, platformSchema, fields, actor);
  }

  public SchemaMetadataKey createSchemaMetadata(String platformName, String schemaName, String datasetName,
      String origin, SchemaMetadata.PlatformSchema platformSchema, SchemaFieldArray fields, Urn actor)
      throws Exception {

    DatasetUrnArray datasetUrns = new DatasetUrnArray();
    datasetUrns.add(toDatasetUrn(platformName, datasetName, origin));

    SchemaMetadata newSchema = new SchemaMetadata().setSchemaName(schemaName)
        .setPlatform(toDataPlatformUrn(platformName))
        .setPermissibleDatasets(datasetUrns)
        .setPlatformSchema(platformSchema)
        .setPlatformSchemaVersion("v-1") // TODO: get version
        .setFields(fields)
        .setPrimaryKeys(new StringArray())
        .setForeignKeysSpecs(new ForeignKeySpecMap())
        .setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
        .setLastModified(new AuditStamp().setActor(actor));

    CreateIdRequest<ComplexResourceKey<SchemaMetadataKey, EmptyRecord>, SchemaMetadata> req =
        _schemaMetadataBuilder.create().input(newSchema).actorParam(actor).build();
    //System.out.println(req);
    //System.out.println(req.getInputRecord());
    //return null;

    IdResponse<ComplexResourceKey<SchemaMetadataKey, EmptyRecord>> resp = _client.sendRequest(req).getResponseEntity();
    return resp.getId().getKey();
  }

  public SchemaMetadataKey updateSchemaFields(String platformName, String datasetName, String origin,
      SchemaFieldArray fields, Urn actor) throws Exception {

    // TODO: do we really need to query the latest schema before merge update
    SchemaMetadata originSchema = getLatestSchemaByDataset(platformName, datasetName, origin);
    SchemaMetadata newSchema = originSchema.copy().setFields(fields).setLastModified(new AuditStamp().setActor(actor));

    // call create with parameter merge to be true
    CreateIdRequest<ComplexResourceKey<SchemaMetadataKey, EmptyRecord>, SchemaMetadata> req =
        _schemaMetadataBuilder.create().input(newSchema).actorParam(actor).mergeWithLatestParam(true).build();

    IdResponse<ComplexResourceKey<SchemaMetadataKey, EmptyRecord>> resp = _client.sendRequest(req).getResponseEntity();
    return resp.getId().getKey();
  }
}
