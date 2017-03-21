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

package wherehows.restli.dataset.client;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetsRequestBuilders;
import com.linkedin.dataset.DeploymentInfoArray;
import com.linkedin.dataset.PlatformNativeType;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;
import java.net.URISyntaxException;
import java.util.Collections;


public class EspressoCreateDataSet {

  public static void create(RestClient _restClient, String iDatasetName, String iUrn)
      throws Exception {

    Dataset newDataset = new Dataset().setName(iDatasetName)
        .setOrigin(FabricType.PROD) // This will be the highest among Fabrics, i.e., DeploymentInfos
        .setPlatform(toDataPlatformUrn("espresso"))
        .setDescription("Sample test description")
        .setDeploymentInfos(new DeploymentInfoArray())
        .setTags(new StringArray())
        .setCreated(new AuditStamp().setActor(new Urn(iUrn)))
        .setLastModified(new AuditStamp().setActor(new Urn(iUrn)))
        .setPlatformNativeType(PlatformNativeType.TABLE)
        .setUri(new java.net.URI("http://dummyUri"));


    try {
      IdResponse<ComplexResourceKey<DatasetKey, EmptyRecord>> response =
          _restClient.sendRequest(_datasetsBuilders.create().input(newDataset).build()).getResponseEntity();

      System.out.println(response.getId().getKey());
    } catch (Exception e) {
      System.out.println(e.fillInStackTrace());
    }

  }

  public static DataPlatformUrn toDataPlatformUrn(String platformName) {
    DataPlatformUrn dataPlatformUrn = null;
    try {
      dataPlatformUrn = DataPlatformUrn.deserialize("urn:li:dataPlatform:" + platformName);
    } catch (URISyntaxException ex) {
      // this can never happen
    }
    return dataPlatformUrn;
  }

  public static DatasetUrn toDatasetUrn(String datasetName, DataPlatformUrn dataPlatform, FabricType origin) {
    try {
      return DatasetUrn.createFromUrn(Urn.createFromTuple("dataset", dataPlatform, datasetName, origin));
    } catch (URISyntaxException ex) {
      // this should never happen
    }
    return null;
  }

  private static final DatasetsRequestBuilders _datasetsBuilders = new DatasetsRequestBuilders();
}

