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
package metadata.etl.lineage;

import java.io.IOException;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wherehows.common.Constant;


/**
 * Handle all communication with Azkaban service
 * Created by zsun on 9/8/15.
 */
public class AzServiceCommunicator {
  ObjectMapper jsonReader;
  private String AZKABAN_URL = "";
  private String sessionId = "";
  private String azkabanUserName = "";
  private String azkabanPassword = "";
  private static final Logger logger = LoggerFactory.getLogger(AzServiceCommunicator.class);
  public AzServiceCommunicator(Properties prop)
    throws Exception {
    jsonReader = new ObjectMapper();
    AZKABAN_URL = prop.getProperty(Constant.AZ_SERVICE_URL_KEY);
    azkabanUserName = prop.getProperty(Constant.AZ_SERVICE_USERNAME_KEY);
    azkabanPassword = prop.getProperty(Constant.AZ_SERVICE_PASSWORD_KEY);
    sessionId = getAzkabanSessionId(azkabanUserName, azkabanPassword);
  }

  /**
   * Send request to azkaban server, no CA.
   * @param url
   * @param params
   * @param type
   * @return
   * @throws java.net.URISyntaxException
   * @throws java.io.IOException
   * @throws java.security.KeyStoreException
   * @throws java.security.NoSuchAlgorithmException
   * @throws java.security.KeyManagementException
   */
  private String sendRequest(String url, Map<String, String> params, String type)
    throws Exception {

    HttpRequestBase request;
    if (type.equals("get")) {
      request = new HttpGet(url);
    } else {
      request = new HttpPost(url);
    }

    URIBuilder uriBuilder = new URIBuilder(request.getURI());
    for (Map.Entry<String, String> entry : params.entrySet()) {
      uriBuilder.addParameter(entry.getKey(), entry.getValue());
    }

    uriBuilder.setParameter("session.id", this.sessionId);

    request.setURI(uriBuilder.build());
    request.setHeader("Content-Type", "application/json");

    // trust self-signed certificates
    SSLContextBuilder sslBuilder = new SSLContextBuilder();
    sslBuilder.loadTrustMaterial(null, (TrustStrategy) new TrustSelfSignedStrategy());
    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslBuilder.build());
    CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
    HttpResponse response = httpclient.execute(request);
    String stringResponse = EntityUtils.toString(response.getEntity());

    if (isSessionExpired(stringResponse)) {
      // if session is expired, re-login
      String sessionId = getAzkabanSessionId(this.azkabanUserName, this.azkabanPassword);
      uriBuilder.setParameter("session.id", sessionId);
      request.setURI(uriBuilder.build());
      response = httpclient.execute(request);
      stringResponse = EntityUtils.toString(response.getEntity());
    }
    return stringResponse;
  }

  private boolean isSessionExpired(String response)
    throws IOException {
    JsonNode json = jsonReader.readTree(response);
    if (json.has("error") && json.get("error").asText().equals("session")) {
      logger.error("session expired");
      return true;
    }
    return false;
  }

  /**
   *
   * @param username
   * @param password
   * @return a session id string
   * @throws java.io.IOException
   */
  public String getAzkabanSessionId(String username, String password)
    throws Exception {

    Map<String, String> params = new HashMap<>();
    params.put("action", "login");
    params.put("username", username);
    params.put("password", password);
    String response = sendRequest(AZKABAN_URL, params, "post");

    JsonNode obj = jsonReader.readTree(response);
    String sessionId = "";
    if (obj.has("status") && obj.get("status").asText().equals("success")) {
      sessionId = obj.get("session.id").asText();
    } else {
      logger.error("log in failed, user name : {}", username);
      // throw exception
      throw new Exception("username/password wrong. user : " + username);
    }
    this.sessionId = sessionId;

    return sessionId;
  }

  public String getExecLog(int execId, String jobName)
    throws Exception {
    return getExecLog(execId, jobName, "0", "1000000");
  }

  public String getExecLog(int execId, String jobName, String offset, String length)
    throws Exception {

    Map<String, String> paramsMap = new HashMap<>();
    // TODO try with session id, if it expired, re-login
    paramsMap.put("ajax", "fetchExecJobLogs");
    paramsMap.put("execid", String.valueOf(execId));
    paramsMap.put("jobId", jobName);
    paramsMap.put("offset", offset);
    paramsMap.put("length", length);
    String url = AZKABAN_URL + "/executor";
    String response = sendRequest(url, paramsMap, "get");

    // retrieve from json
    JsonNode obj = jsonReader.readTree(response);
    String execLog = obj.get("data").asText();

    return execLog;
  }
}
