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
package dao;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesGetRequestBuilder;
import com.linkedin.dataset.DatasetPrivacyCompliancePoliciesRequestBuilders;
import com.linkedin.dataset.DatasetsGetRequestBuilder;
import com.linkedin.dataset.DatasetsRequestBuilders;
import com.linkedin.dataset.PrivacyCompliancePolicy;
import com.linkedin.dataset.PrivacyCompliancePolicyKey;
import com.linkedin.dataset.SchemaField;
import com.linkedin.dataset.SchemaFieldArray;
import com.linkedin.dataset.SchemaMetadata;
import com.linkedin.dataset.SchemaMetadataFindByDatasetRequestBuilder;
import com.linkedin.dataset.SchemaMetadataGetRequestBuilder;
import com.linkedin.dataset.SchemaMetadataKey;
import com.linkedin.dataset.SchemaMetadataRequestBuilders;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import models.DatasetColumn;
import play.Play;

import static wherehows.restli.util.UrnUtil.*;


public class MetadataStoreDao {

  private static final String MetadataStoreURL =
      Play.application().configuration().getString("wherehows.restli.server.url");

  private static final HttpClientFactory http = new HttpClientFactory();
  private static final Client r2Client =
      new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));

  private static final RestClient _client = new RestClient(r2Client, MetadataStoreURL);

  private static final DatasetsRequestBuilders _datasetsBuilders = new DatasetsRequestBuilders();

  private static final SchemaMetadataRequestBuilders _schemaMetadataBuilder = new SchemaMetadataRequestBuilders();

  private static final DatasetPrivacyCompliancePoliciesRequestBuilders _privacyComplianceBuilder =
      new DatasetPrivacyCompliancePoliciesRequestBuilders();

  public static Dataset getDataset(String datasetName, String platformName, String origin) throws Exception {

    DatasetKey key = new DatasetKey().setName(datasetName)
        .setPlatform(toDataPlatformUrn(platformName))
        .setOrigin(toFabricType(origin));

    DatasetsGetRequestBuilder builder = _datasetsBuilders.get();
    Request<Dataset> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    // Send the request and wait for a response
    final ResponseFuture<Dataset> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public static SchemaMetadata getSchemaMetadata(String schemaName, String platformName, long version)
      throws Exception {

    SchemaMetadataKey key = new SchemaMetadataKey().setSchemaName(schemaName)
        .setPlatform(toDataPlatformUrn(platformName))
        .setVersion(version);

    SchemaMetadataGetRequestBuilder builder = _schemaMetadataBuilder.get();
    Request<SchemaMetadata> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    ResponseFuture<SchemaMetadata> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public static SchemaMetadata getLatestSchemaByDataset(String platformName, String datasetName, String origin)
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

  public static List<DatasetColumn> datasetColumnsMapper(SchemaFieldArray fields) {
    List<DatasetColumn> columns = new ArrayList<>();
    for (SchemaField field : fields) {
      DatasetColumn col = new DatasetColumn();
      col.fieldName = field.getFieldPath();
      col.fullFieldPath = field.getFieldPath();
      col.dataType = field.getNativeDataType();
      col.comment = field.getDescription();
      columns.add(col);
    }
    return columns;
  }

  public static SchemaMetadata getLatestSchemaByWhUrn(String urn) throws Exception {
    String[] urnParts = splitWhUrn(urn);
    return getLatestSchemaByDataset(urnParts[0], urnParts[1], "PROD");
  }

  public static PrivacyCompliancePolicy getPrivacyCompliancePolicy(String platformName, String datasetName,
      String origin, long version) throws Exception {

    DatasetUrn urn = toDatasetUrn(platformName, datasetName, origin);
    PrivacyCompliancePolicyKey key = new PrivacyCompliancePolicyKey().setDataset(urn).setVersion(version);

    DatasetPrivacyCompliancePoliciesGetRequestBuilder builder = _privacyComplianceBuilder.get();
    Request<PrivacyCompliancePolicy> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    ResponseFuture<PrivacyCompliancePolicy> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }
}
