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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;


@Slf4j
public class HttpUtil {

  private static final ObjectMapper _OM = new ObjectMapper();

  private HttpUtil() {
  }

  public static JsonNode httpPostRequest(String url, JsonNode content) throws IOException {
    CloseableHttpClient client = HttpClientFactory.createHttpClient();
    HttpPost httpPost = new HttpPost(url);

    httpPost.setEntity(new StringEntity(content.toString()));
    httpPost.setHeader("Accept", "application/json");
    httpPost.setHeader("Content-type", "application/json");

    CloseableHttpResponse response = client.execute(httpPost);
    if (response.getStatusLine().getStatusCode() != 200) {
      log.error("ES request fail: {}", response.getStatusLine().getStatusCode());
      return null;
    }
    JsonNode respJson = _OM.readTree(EntityUtils.toString(response.getEntity()));
    client.close();

    return respJson;
  }
}
