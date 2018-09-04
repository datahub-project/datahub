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
package wherehows.util;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;


public class HttpClientFactory {

  private static final int DEFAULT_TIMEOUT = 1000;  // default 1s timeout

  private HttpClientFactory() {
  }

  public static CloseableHttpClient createHttpClient() {
    return createHttpClient(DEFAULT_TIMEOUT);
  }

  public static CloseableHttpClient createHttpClient(int timeoutMillis) {
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(timeoutMillis)
        .setConnectionRequestTimeout(timeoutMillis)
        .setSocketTimeout(timeoutMillis)
        .build();

    return HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
  }
}
