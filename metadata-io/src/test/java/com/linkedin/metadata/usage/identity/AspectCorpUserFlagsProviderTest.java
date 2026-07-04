package com.linkedin.metadata.usage.identity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.usage.UsageTestFixtures;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider.CorpUserFlags;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AspectCorpUserFlagsProviderTest {

  private final AspectCorpUserFlagsProvider provider = new AspectCorpUserFlagsProvider();

  @Test
  public void testLegacyMethodsReturnFalse() {
    Assert.assertFalse(provider.isSystemCorpUser(UsageTestFixtures.REGULAR_CORP_USER_URN));
    Assert.assertFalse(provider.isSupportUser(UsageTestFixtures.REGULAR_CORP_USER_URN));
  }

  @Test
  public void testNonCorpUserEntityReturnsNone() {
    CorpUserFlags flags =
        provider.resolveWithContext(
            TestOperationContexts.systemContextNoValidate(),
            "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)");
    Assert.assertEquals(flags, CorpUserFlags.NONE);
  }

  @Test
  public void testMissingAspectReturnsNone() throws Exception {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestSystemAspect(
            any(OperationContext.class),
            eq(com.linkedin.common.urn.UrnUtils.getUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(null);
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);

    CorpUserFlags flags =
        provider.resolveWithContext(opContext, UsageTestFixtures.REGULAR_CORP_USER_URN);
    Assert.assertEquals(flags, CorpUserFlags.NONE);
  }

  @Test
  public void testSystemAndSupportFlagsLoadedFromAspect() throws Exception {
    CorpUserInfo corpUserInfo = new CorpUserInfo();
    corpUserInfo.setSystem(true);
    corpUserInfo.data().put("isSupportUser", true);

    SystemAspect systemAspect = mock(SystemAspect.class);
    when(systemAspect.getAspect(CorpUserInfo.class)).thenReturn(corpUserInfo);

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestSystemAspect(
            any(OperationContext.class),
            eq(com.linkedin.common.urn.UrnUtils.getUrn(UsageTestFixtures.REGULAR_CORP_USER_URN)),
            eq(Constants.CORP_USER_INFO_ASPECT_NAME)))
        .thenReturn(systemAspect);
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);

    CorpUserFlags flags =
        provider.resolveWithContext(opContext, UsageTestFixtures.REGULAR_CORP_USER_URN);
    Assert.assertTrue(flags.system());
    Assert.assertTrue(flags.supportUser());
  }

  @Test
  public void testInvalidUrnFailsOpenToNone() {
    CorpUserFlags flags =
        provider.resolveWithContext(
            TestOperationContexts.systemContextNoValidate(), "not-a-valid-urn");
    Assert.assertEquals(flags, CorpUserFlags.NONE);
  }
}
