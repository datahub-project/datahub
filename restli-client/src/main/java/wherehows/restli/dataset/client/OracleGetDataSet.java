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

import com.linkedin.common.FabricType;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.util.None;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import com.linkedin.dataset.DatasetsGetRequestBuilder;
import com.linkedin.dataset.DatasetsRequestBuilders;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.net.URISyntaxException;
import java.util.Collections;


public class OracleGetDataSet {

  public static long get(RestClient _restClient, String datasetName, String platformName) throws Exception{

    DatasetKey
        lGetDatasetKey = new DatasetKey().setName(datasetName).setPlatform(toDataPlatformUrn(platformName)).setOrigin(
        FabricType.PROD);

    DatasetsGetRequestBuilder getBuilder = _datasetsBuilders.get();
    Request<Dataset> getReq = getBuilder.id(new ComplexResourceKey<>(lGetDatasetKey,new EmptyRecord())).build();

    // Send the request and wait for a response
    final ResponseFuture<Dataset> getFuture = _restClient.sendRequest(getReq);
    final Response<Dataset> getResp = getFuture.getResponse();

    return getResp.getEntity().getId();
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

  private static final DatasetsRequestBuilders _datasetsBuilders = new DatasetsRequestBuilders();
}
