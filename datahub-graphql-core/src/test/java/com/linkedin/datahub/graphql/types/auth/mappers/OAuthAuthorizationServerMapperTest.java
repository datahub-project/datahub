package com.linkedin.datahub.graphql.types.auth.mappers;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuthLocation;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.generated.TokenAuthMethod;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.oauth.OAuthAuthorizationServerProperties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OAuthAuthorizationServerMapperTest {

  private QueryContext queryContext;

  @BeforeMethod
  public void setup() {
    queryContext = mock(QueryContext.class);
  }

  @Test
  public void testMapOAuthServerWithFullConfig() {
    // Setup
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:glean");

    // Create OAuthAuthorizationServerProperties
    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Glean OAuth Server");
    props.setDescription("OAuth server for Glean integration");

    // OAuth config
    props.setClientId("glean-client-id");
    props.setClientSecretUrn(UrnUtils.getUrn("urn:li:dataHubSecret:glean-secret"));
    props.setAuthorizationUrl("https://glean.com/oauth/authorize");
    props.setTokenUrl("https://glean.com/oauth/token");
    props.setScopes(new StringArray("read", "write", "search"));
    props.setTokenAuthMethod(com.linkedin.oauth.TokenAuthMethod.POST_BODY);

    // Auth injection config
    props.setAuthLocation(com.linkedin.oauth.AuthLocation.HEADER);
    props.setAuthHeaderName("Authorization");
    props.setAuthScheme("Bearer");

    // Create Status aspect
    Status status = new Status();
    status.setRemoved(false);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify basic fields
    assertNotNull(result);
    assertEquals(result.getUrn(), serverUrn.toString());

    // Verify properties
    assertNotNull(result.getProperties());
    assertEquals(result.getProperties().getDisplayName(), "Glean OAuth Server");
    assertEquals(result.getProperties().getDescription(), "OAuth server for Glean integration");

    // Verify OAuth config
    assertEquals(result.getProperties().getClientId(), "glean-client-id");
    assertTrue(result.getProperties().getHasClientSecret()); // Should indicate secret exists
    assertEquals(result.getProperties().getAuthorizationUrl(), "https://glean.com/oauth/authorize");
    assertEquals(result.getProperties().getTokenUrl(), "https://glean.com/oauth/token");
    assertNotNull(result.getProperties().getScopes());
    assertEquals(result.getProperties().getScopes().size(), 3);
    assertTrue(result.getProperties().getScopes().contains("read"));
    assertTrue(result.getProperties().getScopes().contains("write"));
    assertTrue(result.getProperties().getScopes().contains("search"));
    assertEquals(result.getProperties().getTokenAuthMethod(), TokenAuthMethod.POST_BODY);

    // Verify auth injection config
    assertEquals(result.getProperties().getAuthLocation(), AuthLocation.HEADER);
    assertEquals(result.getProperties().getAuthHeaderName(), "Authorization");
    assertEquals(result.getProperties().getAuthScheme(), "Bearer");

    // Verify status
    assertNotNull(result.getStatus());
    assertFalse(result.getStatus().getRemoved());
  }

  @Test
  public void testMapOAuthServerWithApiKeySupport() {
    // Setup
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:internal-api");

    // Create properties for OAuth with custom header
    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Custom Header OAuth");

    // Custom auth header config
    props.setAuthLocation(com.linkedin.oauth.AuthLocation.HEADER);
    props.setAuthHeaderName("X-Custom-Auth");
    // No auth scheme

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getDisplayName(), "Custom Header OAuth");
    assertEquals(result.getProperties().getAuthHeaderName(), "X-Custom-Auth");
  }

  @Test
  public void testMapOAuthServerWithQueryParamAuth() {
    // Setup - server that uses query param auth
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:queryauth");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Query Param Auth Server");

    props.setAuthLocation(com.linkedin.oauth.AuthLocation.QUERY_PARAM);
    props.setAuthQueryParam("access_token");

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getAuthQueryParam(), "access_token");
  }

  @Test
  public void testMapOAuthServerWithCustomScheme() {
    // Setup - OAuth with custom scheme
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:custom-scheme");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Custom Scheme OAuth");

    props.setAuthLocation(com.linkedin.oauth.AuthLocation.HEADER);
    props.setAuthHeaderName("Authorization");
    props.setAuthScheme("CustomToken");

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getAuthScheme(), "CustomToken");
    assertEquals(result.getProperties().getAuthLocation(), AuthLocation.HEADER);
  }

  @Test
  public void testMapOAuthServerMinimalConfig() {
    // Setup - minimal required config
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:minimal");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Minimal Server");

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getDisplayName(), "Minimal Server");
    assertNull(result.getProperties().getDescription());
    assertNull(result.getProperties().getClientId());
    assertFalse(result.getProperties().getHasClientSecret());
    assertNull(result.getProperties().getAuthorizationUrl());
    assertNull(result.getProperties().getTokenUrl());
  }

  @Test
  public void testMapOAuthServerWithBasicAuth() {
    // Setup - server using Basic auth for token endpoint
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:basic-auth");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Basic Auth Server");
    props.setTokenAuthMethod(com.linkedin.oauth.TokenAuthMethod.BASIC);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getTokenAuthMethod(), TokenAuthMethod.BASIC);
  }

  @Test
  public void testMapOAuthServerWithNoneTokenAuthMethod() {
    // Setup - public client using NONE token auth method
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:public-client");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Public Client Server");
    props.setTokenAuthMethod(com.linkedin.oauth.TokenAuthMethod.NONE);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getTokenAuthMethod(), TokenAuthMethod.NONE);
  }

  @Test
  public void testMapOAuthServerWithRemovedStatus() {
    // Setup - soft-deleted OAuth server
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:deleted");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Deleted Server");

    // Create Status with removed=true
    Status status = new Status();
    status.setRemoved(true);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    aspects.put(
        Constants.STATUS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(status.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getStatus());
    assertTrue(result.getStatus().getRemoved());
  }

  @Test
  public void testMapOAuthServerWithNoClientSecret() {
    // Setup - server without client secret (public client)
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:no-secret");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("No Secret Server");
    props.setClientId("public-client-id");
    // No clientSecretUrn set

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getClientId(), "public-client-id");
    assertFalse(result.getProperties().getHasClientSecret());
  }

  @Test
  public void testMapOAuthServerWithEmptyScopes() {
    // Setup - server with empty scopes array
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:empty-scopes");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Empty Scopes Server");
    props.setScopes(new StringArray());

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertNotNull(result.getProperties().getScopes());
    assertTrue(result.getProperties().getScopes().isEmpty());
  }

  @Test
  public void testMapOAuthServerWithAllAuthInjectionSettings() {
    // Setup - server with all auth injection settings configured
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:full-injection");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Full Auth Injection Server");

    // Set all auth injection fields
    props.setAuthLocation(com.linkedin.oauth.AuthLocation.HEADER);
    props.setAuthHeaderName("X-Custom-Auth");
    props.setAuthScheme("CustomBearer");
    props.setAuthQueryParam("fallback_token"); // Set even though location is HEADER

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getAuthLocation(), AuthLocation.HEADER);
    assertEquals(result.getProperties().getAuthHeaderName(), "X-Custom-Auth");
    assertEquals(result.getProperties().getAuthScheme(), "CustomBearer");
    assertEquals(result.getProperties().getAuthQueryParam(), "fallback_token");
  }

  // Note: DCR (Dynamic Client Registration) fields test skipped
  // as registrationUrl and initialAccessTokenUrn are planned for Phase 2

  @Test
  public void testMapOAuthServerWithLongDescription() {
    // Setup - server with long description
    Urn serverUrn = UrnUtils.getUrn("urn:li:oauthAuthorizationServer:long-desc");

    OAuthAuthorizationServerProperties props = new OAuthAuthorizationServerProperties();
    props.setDisplayName("Long Description Server");
    String longDescription =
        "This is a very long description that contains multiple sentences. "
            + "It explains in detail what this OAuth server does and how it should be used. "
            + "The description can contain special characters like: & < > \" ' and even newlines.\n"
            + "This tests that long text is handled properly by the mapper.";
    props.setDescription(longDescription);

    // Build EntityResponse
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(serverUrn);
    entityResponse.setEntityName(Constants.OAUTH_AUTHORIZATION_SERVER_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    // Execute
    OAuthAuthorizationServer result =
        OAuthAuthorizationServerMapper.map(queryContext, entityResponse);

    // Verify
    assertNotNull(result);
    assertEquals(result.getProperties().getDescription(), longDescription);
  }
}
