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

import junit.framework.Assert;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;


/**
 * Created by zsun on 9/3/15.
 */
public class HadoopNameNodeExtractorTest {
  HadoopNameNodeExtractor he;

  @BeforeTest
  public void setUp() {
    try {
      he = new HadoopNameNodeExtractor(new LineageTest().properties);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {"needConfig"})
  public void testGetConf()
    throws Exception {
    String result = he.getConfFromHadoop("job_1437229398924_817615");
    // job_1437229398924_817551
    System.err.println(result); // always will not found for old job
    Assert.assertNotNull(result);
  }
}
