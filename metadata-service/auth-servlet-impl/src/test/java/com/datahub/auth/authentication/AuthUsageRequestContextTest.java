package com.datahub.auth.authentication;

import static org.testng.Assert.assertEquals;

import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import org.springframework.http.HttpHeaders;
import org.testng.annotations.Test;

public class AuthUsageRequestContextTest {

  @Test
  public void testOpenapiUsageWithoutHeadersUsesEmptyClientMetadata() {
    RequestContext requestContext =
        AuthUsageRequestContext.openapiUsage(
                "urn:li:corpuser:test", null, null, "req-1", UsageOperation.OTHER_OPERATIONS)
            .build();

    assertEquals(requestContext.getActorUrn(), "urn:li:corpuser:test");
    assertEquals(requestContext.getRequestID(), "req-1");
    assertEquals(requestContext.getUsageOperation(), UsageOperation.OTHER_OPERATIONS.key());
    assertEquals(requestContext.getRequestAPI(), RequestContext.RequestAPI.OPENAPI);
    assertEquals(requestContext.getSourceIP(), "");
    assertEquals(requestContext.getUserAgent(), "");
  }

  @Test
  public void testOpenapiUsageWithHeadersExtractsForwardedForAndUserAgent() {
    HttpHeaders headers = new HttpHeaders();
    headers.add("X-Forwarded-For", "203.0.113.10");
    headers.add(HttpHeaders.USER_AGENT, "DataHub-Test/1.0");

    RequestContext requestContext =
        AuthUsageRequestContext.openapiUsage(
                "urn:li:corpuser:test", headers, "req-2", UsageOperation.METADATA_READ)
            .build();

    assertEquals(requestContext.getSourceIP(), "203.0.113.10");
    assertEquals(requestContext.getUserAgent(), "DataHub-Test/1.0");
    assertEquals(requestContext.getUsageOperation(), UsageOperation.METADATA_READ.key());
  }

  @Test
  public void testOpenapiUsageWithNullHeadersDelegatesToExplicitFields() {
    RequestContext requestContext =
        AuthUsageRequestContext.openapiUsage(
                "urn:li:corpuser:test",
                "198.51.100.4",
                "Mozilla/5.0",
                "req-3",
                UsageOperation.OTHER_WRITE)
            .build();

    assertEquals(requestContext.getSourceIP(), "198.51.100.4");
    assertEquals(requestContext.getUserAgent(), "Mozilla/5.0");
    assertEquals(requestContext.getUsageOperation(), UsageOperation.OTHER_WRITE.key());
  }
}
