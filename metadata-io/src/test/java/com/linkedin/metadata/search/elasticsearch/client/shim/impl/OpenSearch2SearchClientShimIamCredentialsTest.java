package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.http.HttpRequestInterceptor;
import org.opensearch.client.RestHighLevelClient;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class OpenSearch2SearchClientShimIamCredentialsTest {

  @Test
  public void signingInterceptorHoldsTheConfiguredCredentialsProvider() throws Exception {
    AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class);
    OpenSearch2SearchClientShim shim =
        OpenSearch2SearchClientShim.forTest(
            mock(RestHighLevelClient.class),
            new ShimConfigurationBuilder()
                .withAwsIamAuth(true, "us-east-1")
                .withAwsCredentialsProvider(provider)
                .build());

    HttpRequestInterceptor interceptor = invokeSigningInterceptor(shim, "us-east-1");
    Field field =
        AwsRequestSigningApacheInterceptor.class.getDeclaredField("awsCredentialsProvider");
    field.setAccessible(true);
    assertSame(provider, field.get(interceptor));
  }

  @Test
  public void signingInterceptorRejectsIamAuthWithoutCredentialsProvider() throws Exception {
    OpenSearch2SearchClientShim shim =
        OpenSearch2SearchClientShim.forTest(
            mock(RestHighLevelClient.class),
            new ShimConfigurationBuilder().withAwsIamAuth(true, "us-east-1").build());

    expectThrows(IllegalStateException.class, () -> invokeSigningInterceptor(shim, "us-east-1"));
  }

  private static HttpRequestInterceptor invokeSigningInterceptor(
      OpenSearch2SearchClientShim shim, String region) throws Exception {
    Method method =
        OpenSearch2SearchClientShim.class.getDeclaredMethod(
            "getAwsRequestSigningInterceptor", String.class);
    method.setAccessible(true);
    try {
      return (HttpRequestInterceptor) method.invoke(shim, region);
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw e;
    }
  }
}
