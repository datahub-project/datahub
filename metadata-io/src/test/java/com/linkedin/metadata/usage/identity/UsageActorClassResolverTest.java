package com.linkedin.metadata.usage.identity;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider.CorpUserFlags;
import com.linkedin.metadata.usage.instrumentation.UsageRequestState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UsageActorClassResolverTest {

  private final UsageActorClassResolver defaultResolver =
      new UsageActorClassResolver(new AspectCorpUserFlagsProvider());

  @BeforeMethod
  @AfterMethod
  public void clearSessionFlags() {
    UsageRequestState.clear();
  }

  @Test
  public void testSystemActorUrn() throws Exception {
    Assert.assertEquals(
        defaultResolver.resolve(
            context(Constants.SYSTEM_ACTOR),
            request(Constants.SYSTEM_ACTOR),
            userAuth(Constants.SYSTEM_ACTOR)),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testCorpUsersWithoutFlagsAreRegular() throws Exception {
    Assert.assertEquals(
        defaultResolver.resolve(
            context("urn:li:corpuser:datahub"),
            request("urn:li:corpuser:datahub"),
            userAuth("urn:li:corpuser:datahub")),
        UsageActorClass.REGULAR);
    Assert.assertEquals(
        defaultResolver.resolve(
            context("urn:li:corpuser:admin"),
            request("urn:li:corpuser:admin"),
            userAuth("urn:li:corpuser:admin")),
        UsageActorClass.REGULAR);
  }

  @Test
  public void testSystemCorpUserFlagTakesPrecedenceOverSupportUser() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(true, true));
    Assert.assertEquals(
        resolver.resolve(
            context(UsageTestFixtures.REGULAR_CORP_USER_URN),
            request(UsageTestFixtures.REGULAR_CORP_USER_URN),
            userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN)),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testSystemFlagClassifiesCorpUserAsSystem() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(true, false));
    Assert.assertEquals(
        resolver.resolve(
            context("urn:li:corpuser:datahub"),
            request("urn:li:corpuser:datahub"),
            userAuth("urn:li:corpuser:datahub")),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testRegularCorpUser() throws Exception {
    Assert.assertEquals(
        defaultResolver.resolve(
            context(UsageTestFixtures.REGULAR_CORP_USER_URN),
            request(UsageTestFixtures.REGULAR_CORP_USER_URN),
            userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN)),
        UsageActorClass.REGULAR);
  }

  @Test
  public void testSupportUserCorpUserIsSupport() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(false, true));
    Assert.assertEquals(
        resolver.resolve(
            context(UsageTestFixtures.REGULAR_CORP_USER_URN),
            request(UsageTestFixtures.REGULAR_CORP_USER_URN),
            userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN)),
        UsageActorClass.SUPPORT);
  }

  @Test
  public void testSystemCorpUserIsSystem() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(true, false));
    Assert.assertEquals(
        resolver.resolve(
            context(UsageTestFixtures.REGULAR_CORP_USER_URN),
            request(UsageTestFixtures.REGULAR_CORP_USER_URN),
            userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN)),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testAnonymousUsageIdentityIsSystem() throws Exception {
    Assert.assertEquals(
        defaultResolver.resolve(
            context(Constants.ANONYMOUS_ACTOR),
            request(Constants.ANONYMOUS_ACTOR),
            userAuth(Constants.ANONYMOUS_ACTOR)),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testUnknownUsageIdentityIsRegular() throws Exception {
    // Stable unknown principal (unparsed JWT subject, etc.) — regular, not system.
    Assert.assertEquals(
        defaultResolver.resolve(
            context(Constants.UNKNOWN_ACTOR),
            request(Constants.UNKNOWN_ACTOR),
            userAuth(Constants.SYSTEM_ACTOR)),
        UsageActorClass.REGULAR);
  }

  @Test
  public void testResolvesCorpUserFromActorUrnWhenUsageIdentityMissing() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(false, true));
    RequestContext requestContext =
        RequestContext.builder()
            .actorUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)
            .sourceIP("127.0.0.1")
            .requestAPI(RequestContext.RequestAPI.OPENAPI)
            .requestID("test")
            .build();
    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .requestContext(requestContext)
            .build(userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN), true);
    Authentication auth = userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN);

    Assert.assertEquals(resolver.resolve(opContext, requestContext, auth), UsageActorClass.SUPPORT);
  }

  @Test
  public void testSessionCorpUserFlagsCachedPerRequest() throws Exception {
    CountingCorpUserFlagsProvider provider = new CountingCorpUserFlagsProvider(false, false);
    UsageActorClassResolver resolver = new UsageActorClassResolver(provider);
    OperationContext opContext = context(UsageTestFixtures.REGULAR_CORP_USER_URN);
    RequestContext requestContext = request(UsageTestFixtures.REGULAR_CORP_USER_URN);
    Authentication auth = userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN);

    resolver.resolve(opContext, requestContext, auth);
    resolver.resolve(opContext, requestContext, auth);
    Assert.assertEquals(provider.resolveCount, 1);

    UsageRequestState.clear();
    resolver.resolve(opContext, requestContext, auth);
    Assert.assertEquals(provider.resolveCount, 2);
  }

  @Test
  public void testNullSessionActorFallsBackToRegular() throws Exception {
    OperationContext opContext = context(UsageTestFixtures.REGULAR_CORP_USER_URN);
    RequestContext requestContext = request(UsageTestFixtures.REGULAR_CORP_USER_URN);
    Authentication auth = org.mockito.Mockito.mock(Authentication.class);
    org.mockito.Mockito.when(auth.getActor()).thenReturn(null);
    Assert.assertEquals(
        defaultResolver.resolve(opContext, requestContext, auth), UsageActorClass.REGULAR);
  }

  @Test
  public void testSystemAuthWithAttributedCorpUserClassifiesAttributedIdentity() throws Exception {
    UsageActorClassResolver resolver =
        new UsageActorClassResolver(new FixedCorpUserFlagsProvider(false, true));
    RequestContext requestContext = request(UsageTestFixtures.REGULAR_CORP_USER_URN);
    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .requestContext(requestContext)
            .build(TestOperationContexts.systemContextNoValidate().getSessionActorContext(), false);
    Assert.assertEquals(
        resolver.resolve(opContext, requestContext, userAuth(Constants.SYSTEM_ACTOR)),
        UsageActorClass.SUPPORT);
  }

  @Test
  public void testSystemAuthWithoutAttributedIdentityRemainsSystem() throws Exception {
    RequestContext requestContext = request(Constants.SYSTEM_ACTOR);
    OperationContext opContext =
        TestOperationContexts.systemContextNoValidate().toBuilder()
            .requestContext(requestContext)
            .build(TestOperationContexts.systemContextNoValidate().getSessionActorContext(), false);
    Assert.assertEquals(
        defaultResolver.resolve(opContext, requestContext, userAuth(Constants.SYSTEM_ACTOR)),
        UsageActorClass.SYSTEM);
  }

  @Test
  public void testNonSessionCorpUserLookupCachedPerRequest() throws Exception {
    CountingCorpUserFlagsProvider provider = new CountingCorpUserFlagsProvider(false, false);
    UsageActorClassResolver resolver = new UsageActorClassResolver(provider);
    String otherUser = "urn:li:corpuser:other";
    OperationContext opContext = context(UsageTestFixtures.REGULAR_CORP_USER_URN);
    RequestContext requestContext = request(otherUser);
    Authentication auth = userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN);

    resolver.resolve(opContext, requestContext, auth);
    resolver.resolve(opContext, requestContext, auth);
    Assert.assertEquals(provider.resolveCount, 1);

    UsageRequestState.clear();
    resolver.resolve(opContext, requestContext, auth);
    Assert.assertEquals(provider.resolveCount, 2);
  }

  @Test
  public void testResolveCachedAcrossRecordRequestAndResponse() throws Exception {
    CountingCorpUserFlagsProvider provider = new CountingCorpUserFlagsProvider(false, false);
    UsageActorClassResolver resolver = new UsageActorClassResolver(provider);
    OperationContext opContext = context(UsageTestFixtures.REGULAR_CORP_USER_URN);
    RequestContext requestContext = request(UsageTestFixtures.REGULAR_CORP_USER_URN);
    Authentication auth = userAuth(UsageTestFixtures.REGULAR_CORP_USER_URN);

    resolver.resolve(opContext, requestContext, auth);
    resolver.resolve(opContext, requestContext, auth);
    Assert.assertEquals(provider.resolveCount, 1);
  }

  private static final class CountingCorpUserFlagsProvider implements CorpUserFlagsProvider {
    private final CorpUserFlags flags;
    private int resolveCount;

    private CountingCorpUserFlagsProvider(boolean system, boolean supportUser) {
      this.flags = new CorpUserFlags(system, supportUser);
    }

    @Override
    public boolean isSystemCorpUser(String corpUserUrn) {
      return flags.system();
    }

    @Override
    public boolean isSupportUser(String corpUserUrn) {
      return flags.supportUser();
    }

    @Override
    public CorpUserFlags resolveWithContext(OperationContext opContext, String corpUserUrn) {
      resolveCount++;
      return flags;
    }
  }

  private static final class FixedCorpUserFlagsProvider implements CorpUserFlagsProvider {
    private final CorpUserFlags flags;

    private FixedCorpUserFlagsProvider(boolean system, boolean supportUser) {
      this.flags = new CorpUserFlags(system, supportUser);
    }

    @Override
    public boolean isSystemCorpUser(String corpUserUrn) {
      return flags.system();
    }

    @Override
    public boolean isSupportUser(String corpUserUrn) {
      return flags.supportUser();
    }

    @Override
    public CorpUserFlags resolveWithContext(OperationContext opContext, String corpUserUrn) {
      return flags;
    }
  }

  private static OperationContext context(String actorUrn) throws Exception {
    return TestOperationContexts.systemContextNoValidate().toBuilder()
        .requestContext(request(actorUrn))
        .build(userAuth(actorUrn), true);
  }

  private static RequestContext request(String actorUrn) {
    return RequestContext.builder()
        .actorUrn(actorUrn)
        .sourceIP("127.0.0.1")
        .requestAPI(RequestContext.RequestAPI.OPENAPI)
        .requestID("test")
        .usageIdentity(actorUrn)
        .build();
  }

  private static Authentication userAuth(String actorUrn) {
    String actorId =
        actorUrn.startsWith("urn:li:corpuser:")
            ? actorUrn.substring("urn:li:corpuser:".length())
            : actorUrn;
    return new Authentication(new Actor(ActorType.USER, actorId), "Basic test");
  }
}
