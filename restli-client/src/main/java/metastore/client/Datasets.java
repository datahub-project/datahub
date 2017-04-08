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
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetsGetRequestBuilder;
import com.linkedin.dataset.DatasetsRequestBuilders;
import com.linkedin.dataset.DeploymentInfoArray;
import com.linkedin.dataset.VersionedDataset;
import com.linkedin.dataset.VersionedDatasetsRequestBuilders;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.PartialUpdateRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.util.PatchGenerator;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import com.linkedin.restli.common.PatchRequest;
import java.util.List;

import static metastore.util.UrnUtil.*;


public class Datasets {

  private final RestClient _client;

  private static final DatasetsRequestBuilders _datasetsBuilder = new DatasetsRequestBuilders();

  private static final VersionedDatasetsRequestBuilders _versionedDatasetsBuilder =
      new VersionedDatasetsRequestBuilders();

  public Datasets(RestClient metadataStoreClient) {
    _client = metadataStoreClient;
  }

  public DatasetKey toDatasetKey(String platformName, String datasetName, String origin) throws Exception {
    return new DatasetKey().setName(datasetName)
        .setPlatform(toDataPlatformUrn(platformName))
        .setOrigin(toFabricType(origin));
  }

  public Dataset getDataset(String platformName, String datasetName, String origin) throws Exception {

    DatasetKey key = toDatasetKey(platformName, datasetName, origin);

    DatasetsGetRequestBuilder builder = _datasetsBuilder.get();
    Request<Dataset> req = builder.id(new ComplexResourceKey<>(key, new EmptyRecord())).build();

    // Send the request and wait for a response
    final ResponseFuture<Dataset> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public VersionedDataset getVersionedDataset(String platformName, String datasetName, String origin) throws Exception {

    DatasetKey key = toDatasetKey(platformName, datasetName, origin);

    Request<Dataset> reqDataset = _datasetsBuilder.get().id(new ComplexResourceKey<>(key, new EmptyRecord())).build();
    Long datasetId = _client.sendRequest(reqDataset).getResponse().getEntity().getId();

    Request<VersionedDataset> req = _versionedDatasetsBuilder.get()
        .id(datasetId)
        .datasetKeyKey(new ComplexResourceKey<>(key, new EmptyRecord()))
        .build();
    ResponseFuture<VersionedDataset> responseFuture = _client.sendRequest(req);
    return responseFuture.getResponse().getEntity();
  }

  public DatasetKey createDataset(String platformName, String datasetName, String origin, String nativeType,
      String description, List<String> tags, String uri, Urn actor) throws Exception {

    Dataset newDataset = new Dataset().setPlatform(toDataPlatformUrn(platformName))
        .setName(datasetName)
        .setOrigin(toFabricType(origin))
        .setDescription(description)
        .setDeploymentInfos(new DeploymentInfoArray()) // TODO: add deployment info when creating dataset
        .setTags(new StringArray(tags))
        .setCreated(new AuditStamp().setActor(actor).setTime(System.currentTimeMillis()))
        .setLastModified(new AuditStamp().setActor(actor))
        .setPlatformNativeType(toPlatformNativeType(nativeType))
        .setUri(new java.net.URI(uri));

    CreateIdRequest<ComplexResourceKey<DatasetKey, EmptyRecord>, Dataset> req =
        _datasetsBuilder.create().input(newDataset).build();
    IdResponse<ComplexResourceKey<DatasetKey, EmptyRecord>> resp = _client.sendRequest(req).getResponseEntity();
    return resp.getId().getKey();
  }

  public void partialUpdateDataset(String platformName, String datasetName, String origin, String description,
      List<String> tags, String uri, Urn actor) throws Exception {

    Dataset originDs = getDataset(platformName, datasetName, origin);
    partialUpdateDataset(platformName, datasetName, origin, originDs, description, tags, uri, actor);
  }

  public void partialUpdateDataset(String platformName, String datasetName, String origin, Dataset originDs,
      String description, List<String> tags, String uri, Urn actor) throws Exception {

    DatasetKey key = toDatasetKey(platformName, datasetName, origin);
    Dataset newDs = originDs.copy()
        .setDescription(description)
        .setTags(new StringArray(tags))
        .setUri(new java.net.URI(uri))
        .setLastModified(new AuditStamp().setActor(actor));

    PatchRequest<Dataset> patch = PatchGenerator.diff(originDs, newDs);
    PartialUpdateRequest<Dataset> req =
        _datasetsBuilder.partialUpdate().id(new ComplexResourceKey<>(key, new EmptyRecord())).input(patch).build();

    EmptyRecord resp = _client.sendRequest(req).getResponseEntity();
  }
}
