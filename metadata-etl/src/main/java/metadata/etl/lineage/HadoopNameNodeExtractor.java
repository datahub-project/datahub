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
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;


/**
 * Created by zsun on 9/3/15.
 */
public class HadoopNameNodeExtractor {

  private String serverURL = "";
  private CloseableHttpClient httpClient;
  private static final Logger logger = LoggerFactory.getLogger(HadoopNameNodeExtractor.class);

  /**
   * Use HTTPClient to connect to Hadoop job history server.
   * Need to set the environment for kerberos, keytab...
   * @param prop
   * @throws Exception
   */
  public HadoopNameNodeExtractor(Properties prop)
    throws Exception {
    this.serverURL = prop.getProperty(Constant.AZ_HADOOP_JOBHISTORY_KEY);

    String CURRENT_DIR = System.getProperty("user.dir");
    String WH_HOME = System.getenv("WH_HOME");
    String USER_HOME = System.getenv("HOME") + "/.kerberos";
    String ETC = "/etc";
    String TMP = "/var/tmp" + "/.kerberos";

    String[] allPositions = new String[]{CURRENT_DIR, WH_HOME, USER_HOME, TMP};

    for (String possition : allPositions) {
      String gssFileName = possition + "/gss-jaas.conf";
      File gssFile = new File(gssFileName);
      if (gssFile.exists()) {
        logger.debug("find gss-jaas.conf file in : {}", gssFile.getAbsolutePath());
        System.setProperty("java.security.auth.login.config", gssFile.getAbsolutePath());
        break;
      } else {
        logger.debug("can't find here: {}", gssFile.getAbsolutePath());
      }
    }
    for (String possition : allPositions) {
      String krb5FileName = possition + "/krb5.conf";
      File krb5File = new File(krb5FileName);
      if (krb5File.exists()) {
        logger.debug("find krb5.conf file in : {}", krb5File.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
        break;
      } else {
        logger.debug("can't find here: {}", krb5File.getAbsolutePath());
      }
    }

    if (System.getProperty("java.security.auth.login.config") == null
      || System.getProperty("java.security.krb5.conf") == null) {
      throw new Exception("Can't find java security config files");
    }

    if (logger.isTraceEnabled()) {
      System.setProperty("sun.security.krb5.debug", "true");
    } else {
      System.setProperty("sun.security.krb5.debug", "false");
    }
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

    System.setProperty("java.security.krb5.realm", prop.getProperty("krb5.realm"));
    System.setProperty("java.security.krb5.kdc", prop.getProperty("krb5.kdc"));

    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(200);
    cm.setDefaultMaxPerRoute(100);

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("DUMMY", null));

    Lookup<AuthSchemeProvider> authRegistry =
      RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

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
    logger.debug("get job conf from : {}", url);
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
}
