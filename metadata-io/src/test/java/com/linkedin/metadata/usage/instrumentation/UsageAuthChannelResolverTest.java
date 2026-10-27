package com.linkedin.metadata.usage.instrumentation;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.token.TokenClaimNames;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.context.usage.AuthChannel;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UsageAuthChannelResolverTest {

  private static final String USER = "urn:li:corpuser:datahub";

  @Test
  public void testBasicCredentialsMapToSession() {
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "datahub"), "Basic dXNlcjpwYXNz");

    Assert.assertEquals(
        UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.SESSION);
  }

  @Test
  public void testPersonalAccessTokenMapsToPat() {
    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, "datahub"),
            "Bearer token",
            Map.of(TokenClaimNames.TOKEN_TYPE, "PERSONAL"));

    Assert.assertEquals(UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.PAT);
  }

  @Test
  public void testServiceAccountTokenMapsToPat() {
    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, "ingestion"),
            "Bearer token",
            Map.of(TokenClaimNames.TOKEN_TYPE, "SERVICE_ACCOUNT"));

    Assert.assertEquals(UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.PAT);
  }

  @Test
  public void testBearerSessionTokenMapsToSession() {
    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, "datahub"),
            "Bearer token",
            Map.of(TokenClaimNames.TOKEN_TYPE, "SESSION"));

    Assert.assertEquals(
        UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.SESSION);
  }

  @Test
  public void testOAuth2AccessTokenMapsToOAuth() {
    Authentication authentication =
        new Authentication(
            new Actor(ActorType.USER, "datahub"),
            "Bearer token",
            Map.of(TokenClaimNames.TOKEN_TYPE, "OAUTH2_ACCESS"));

    Assert.assertEquals(UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.OAUTH);
  }

  @Test
  public void testExternalBearerWithoutClaimsMapsToOAuth() {
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "datahub"), "Bearer external-jwt");

    Assert.assertEquals(UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.OAUTH);
  }

  @Test
  public void testSystemAndAnonymousActors() {
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, "datahub"), "Bearer token");

    Assert.assertEquals(
        UsageAuthChannelResolver.resolve(authentication, Constants.SYSTEM_ACTOR),
        AuthChannel.SYSTEM);
    Assert.assertEquals(
        UsageAuthChannelResolver.resolve(authentication, Constants.ANONYMOUS_ACTOR),
        AuthChannel.ANONYMOUS);
  }

  @Test
  public void testMissingCredentialsMapsToUnknown() {
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "datahub"), "");

    Assert.assertEquals(
        UsageAuthChannelResolver.resolve(authentication, USER), AuthChannel.UNKNOWN);
  }
}
