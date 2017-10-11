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

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.BasicSchemeFactory;
import org.apache.http.impl.auth.KerberosSchemeFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import wherehows.common.Constant;


/**
 * Created by zsun on 9/3/15.
 */
@Slf4j
public class HadoopJobHistoryNodeExtractor {

  private String serverURL = "";
  private CloseableHttpClient httpClient;


  /**
   * Use HTTPClient to connect to Hadoop job history server.
   * Need to set the environment for kerberos, keytab...
   * @param prop
   * @throws Exception
   */
  public HadoopJobHistoryNodeExtractor(Properties prop)
    throws Exception {
    this.serverURL = prop.getProperty(Constant.AZ_HADOOP_JOBHISTORY_KEY);

    String CURRENT_DIR = System.getProperty("user.dir");
    String WHZ_KRB5_DIR = System.getenv("WHZ_KRB5_DIR");
    String APP_HOME = System.getenv("APP_HOME");
    String USER_HOME = System.getenv("HOME") + "/.kerberos";

    String[] searchPath = new String[]{CURRENT_DIR, WHZ_KRB5_DIR, APP_HOME, USER_HOME, "/etc"};

    System.setProperty("java.security.auth.login.config", findFileInSearchPath(searchPath, "gss-jaas.conf"));
    System.setProperty("java.security.krb5.conf", findFileInSearchPath(searchPath, "krb5.conf"));

    if (System.getProperty("java.security.auth.login.config") == null
      || System.getProperty("java.security.krb5.conf") == null) {
      log.warn("Can't find Java security config [krb5.conf, gss-jass.conf] for Kerberos! Trying other authentication methods...");
    }

    if (log.isTraceEnabled()) {
      System.setProperty("sun.security.krb5.debug", "true");
    } else {
      System.setProperty("sun.security.krb5.debug", "false");
    }
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

    System.setProperty("java.security.krb5.realm", prop.getProperty(Constant.KRB5_REALM));
    System.setProperty("java.security.krb5.kdc", prop.getProperty(Constant.KRB5_KDC));

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(200);
    cm.setDefaultMaxPerRoute(100);

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("DUMMY", null));

    Lookup<AuthSchemeProvider> authRegistry =
      RegistryBuilder.<AuthSchemeProvider>create()
        .register(AuthSchemes.BASIC, new BasicSchemeFactory())
        .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
        .register(AuthSchemes.KERBEROS, new KerberosSchemeFactory()).build();

    httpClient =
      HttpClients.custom().setDefaultCredentialsProvider(credsProvider).setDefaultAuthSchemeRegistry(authRegistry)
        .setConnectionManager(cm).build();
  }

  /**
   * Get the job conf from hadoop name node
   * @param hadoopJobId
   * @return the lineage info
   * @throws java.io.IOException
   */
  public String getConfFromHadoop(String hadoopJobId)
    throws Exception {
    String url = this.serverURL + "/" + hadoopJobId + "/conf";
    log.debug("get job conf from : {}", url);
    HttpUriRequest request = new HttpGet(url);
    HttpResponse response = httpClient.execute(request);
    HttpEntity entity = response.getEntity();
    String confResult = EntityUtils.toString(entity);
    EntityUtils.consume(entity);
    return confResult;
  }

  public void close()
    throws IOException {
    httpClient.close();
  }

  @Nullable
  private String findFileInSearchPath(@Nonnull String[] searchPath, @Nonnull String filename) {
    for (String path : searchPath) {
      File file = new File(path, filename);
      if (file.exists()) {
        String absolutePath = file.getAbsolutePath();
        log.debug("Found {} file at: {}", filename, absolutePath);
        return absolutePath;
      } else {
        log.debug("{} doesn't exist.", file.getAbsolutePath());
      }
    }
    return null;
  }
}
