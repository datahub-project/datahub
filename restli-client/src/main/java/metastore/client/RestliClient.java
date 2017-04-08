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

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import java.util.Collections;


public class RestliClient {

  private final HttpClientFactory http;
  private final Client r2Client;
  private final RestClient client;

  public RestliClient(String serverUrl) {
    http = new HttpClientFactory();
    r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));
    client = new RestClient(r2Client, serverUrl);
  }

  public RestClient getClient() {
    return client;
  }

  public void shutdown() {
    client.shutdown(new FutureCallback<>());
    http.shutdown(new FutureCallback<>());
  }
}
